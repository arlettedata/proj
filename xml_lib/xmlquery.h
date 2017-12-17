/* *      Streaming XML Parser.
 *
 *      Copyright (c) 2005-2017 by Brian. Kramer
 *
 * This software is provided 'as-is', without any express or implied
 * warranty.  In no event will the authors be held liable for any damages
 * arising from the use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions (known as zlib license):
 *
 * 1. The origin of this software must not be misrepresented; you must not
 *    claim that you wrote the original software. If you use this software
 *    in a product, an acknowledgment in the product documentation would be
 *    appreciated but is not required.
 * 2. Altered source versions must be plainly marked as such, and must not be
 *    misrepresented as being the original software.
 * 3. This notice may not be removed or altered from any source distribution.
 */

#pragma once

#include <algorithm>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "xmlpivot.h"
#include "xmlqueryspec.h"

namespace StreamingXml
{

typedef std::function<void(size_t)> RowCallback;

class XmlQuery
{
public:
    enum Flags
    {
        All = 0x1,
        StoreRows = 0x2,
        InvokeRowCallback = 0x4,
        ParseStopped = 0x8,
        RecycleStorage = 0x10,
    };

    typedef std::pair<XmlRow*, size_t> RowRef;
    typedef std::unordered_map<XmlRow, size_t, XmlRowHash, XmlRowEquals> DistinctRowsMap;
    
    XmlQuery(XmlParserContextPtr context, XmlQuerySpecPtr querySpec)
        : m_flags(0)
        , m_context(context)
        , m_querySpec(querySpec)
        , m_pivoter(context, (IColumnEditor*)querySpec.get())
    {
    }

    void SetFlags(unsigned int flags, bool set = true)
    {
        m_flags = set ? (m_flags | flags) : (m_flags & ~flags);
    }

    bool IsFlagSet(unsigned int flag) const
    {
        return ((m_flags & flag) != 0);
    }

    XmlPivoter& GetPivoter() 
    {
        return m_pivoter;
    }

    const XmlPivoter& GetPivoter() const
    {
        return m_pivoter;
    }

    bool Streaming() const
    {
        return (!Distinct() && !NeedsSorting() && !Aggregated());
    }

    void SetRowCallback(RowCallback rowCallback)
    {
        m_rowCallback = rowCallback;
    }

    void SetIndexedJoin(XmlIndexedRows&& indexedJoin) 
    {
        m_indexedJoin = std::move(indexedJoin);
    }

    void Reset(XmlPassType passType, XmlPassType lastPassType) {
        if (!m_distinctRows.get()) {
            // pivoting can increase number of col, so express as a callback
            auto numValueColsFunc = [this]{
                return m_querySpec->GetNumValueColumns();
            };
            m_distinctRows.reset(new DistinctRowsMap(10, // use GCC's default
                XmlRowHash(numValueColsFunc), XmlRowEquals(numValueColsFunc)));
        }

        SetFlags(ParseStopped, false);

        if (passType == XmlPassType::MainPass) {
            SetFlags(StoreRows, !Streaming());
        }
        else {
            SetFlags(StoreRows, passType == XmlPassType::StoredValuesPass);
        }

        SetFlags(InvokeRowCallback, (passType == lastPassType));
        
        m_pivoter.Reset(); 
        if (passType == XmlPassType::MainPass) {
            m_seqRows.clear();
            m_rowRefs.clear();
            m_aggregates.clear();
            m_distinctRows->clear();
        }

        RemoveRecycledRow();
    }

    void CheckUnreferenced() const 
    {
        return m_pivoter.CheckUnreferenced();
    } 

    const XmlRow& GetRow(size_t rowIdx) const 
    {
        if (Streaming()) {
            // Always at most one row in storage
            assert(rowIdx == 0);
            return m_seqRows[rowIdx];
        }
        if (Distinct()) {
            assert(m_distinctRows.get());
            assert(m_rowRefs.size() == m_distinctRows->size());
            return *m_rowRefs[rowIdx].first;
        }
        if (m_rowRefs.size() == m_seqRows.size()) { // did we sort?
            return *m_rowRefs[rowIdx].first;
        }
        return m_seqRows[rowIdx];
    }

    size_t GetRowRepeatCount(size_t rowIdx) const
    {
        if (Streaming() || Distinct()) {
            return 1;
        }
        if (m_rowRefs.size() == m_seqRows.size()) { // did we sort?
            return m_rowRefs[rowIdx].second;
        }
        return 1;
    }

    void OnEndTag() 
    {
        if (m_pivoter.Enabled()) {
            XmlPivoter::Result result(std::move(m_pivoter.TryPivot(m_seqRows)));
            if (result.WasPivoted()) {
                if (JoinAndCommitRow(m_seqRows.back())) {
                    result.Accept();
                }
                else {
                    if (result.Reject()) {
                        SetFlags(RecycleStorage, true);
                        RemoveRecycledRow();
                    }
                }
            }
        }
    }

    void EmitRow()
    {
        if (m_pivoter.Enabled()) {
            XmlRow& row = AllocRow(m_pivoter.GetPartitionSize());
            XmlExprEvaluator evaluator(m_context);
            m_pivoter.AccumulateRow(row, evaluator);
        }
        else if (!JoinAndCommitRow(AllocRow())) {
            SetFlags(RecycleStorage, true);
            RemoveRecycledRow();
        }
    }

    void OutputStoredRows()
    {
        assert(IsFlagSet(StoreRows)); // Not used when streaming output (i.e. distinct or aggregations not needed)
        assert(IsFlagSet(InvokeRowCallback) && m_rowCallback); // no point in doing this work if there's no consumption

        // Evaluate and store the aggregrate results
        std::vector<XmlColumnPtr> aggregateFilters;
        if (Aggregated()) {
            assert(Distinct());
            size_t maxRows = m_rowRefs.size(); // expected to be populated due to distinct codepath
            if (!NeedsSorting() && m_querySpec->IsFlagSet(XmlQuerySpec::TopNRowsSpecified)) {
                maxRows = std::min(maxRows, m_querySpec->GetTopNRows());
            }
            for (size_t rowIdx = 0; rowIdx < maxRows; rowIdx++) {
                XmlRow& row = *m_rowRefs[rowIdx].first;
                XmlExprEvaluator evaluator(m_context, &m_aggregates[rowIdx]);
                for (auto& column : GetColumns()) {
                    if (column->IsAggregate()) {
                        size_t valueIdx = column->valueIdx;
                        assert(valueIdx != -1);
                        row[valueIdx] = std::move(evaluator.Evaluate(column->expr));
                    }
                    else if (column->IsOutput()) {
                        size_t valueIdx = column->valueIdx;
                        assert(valueIdx != -1);
                        column->expr->SetValue(row[valueIdx]);
                    }
                    if (column->IsAggregate() && column->IsFilter()) {
                        aggregateFilters.push_back(column);
                    }
                }
            }
        }
        else if (!Distinct()) {
            // In the non-distinct case, we've been building up rows in a vector (See StoreRows).
            // Since resizes do not preserve addresses we postpone the m_rowRefs construction until now.
            m_rowRefs.resize(m_seqRows.size());
            for (size_t rowIdx = 0; rowIdx < m_seqRows.size(); rowIdx++) {
                m_rowRefs[rowIdx] = std::make_pair(&m_seqRows[rowIdx], 1);
            }
        }

        if (NeedsSorting()) {
            SortRows();
        }

        size_t maxRows = m_rowRefs.size();
        if (m_querySpec->IsFlagSet(XmlQuerySpec::TopNRowsSpecified)) {
            maxRows = std::min(maxRows, m_querySpec->GetTopNRows());
        }
        for (size_t rowIdx = 0; rowIdx < maxRows; rowIdx++) {
            bool outputRowFlag = true;
            for (auto& column : aggregateFilters) {
                size_t valueIdx = column->valueIdx;
                assert(valueIdx != -1);
                XmlRow& row = *m_rowRefs[rowIdx].first;
                outputRowFlag &= row[valueIdx].bval;
            }
            if (outputRowFlag) {
                m_rowCallback(rowIdx);
            }
        }
    }

private:
    XmlRow& AllocRow(size_t currPartitionSize = 0)
    {
        // For distinct rows, we use a map structure to contain the rows, but we will maintain a single row to build
        // with and recycle. For non-distinct rows, streaming, we also recycle the same row. Otherwise (non-distinct,
        // non-streaming), we want to build a complete table of rows.
        bool keepAllRows = !Distinct() && !Streaming();

        // Ensure a row to write to; we need one when
        // 1: we are keeping all rows and don't have a row to recycle
        // 2: we are continuing a pivoting partition
        // 3: don't have any rows at all yet
        if ((keepAllRows && !IsFlagSet(RecycleStorage)) || (currPartitionSize > 0) || (m_seqRows.size() == 0)) {
            m_seqRows.push_back(std::move(XmlRow(m_querySpec->GetRowSize())));
        }

        // We can recycle this row if we're not batching them
        SetFlags(RecycleStorage, !keepAllRows);

        return m_seqRows.back();
    }

    bool JoinAndCommitRow(XmlRow& row) // returns false if filter fails
    {        
        bool committed = false;

        // Get join table rows, if applicable
        bool leftSideOfJoin = m_querySpec->IsFlagSet(XmlQuerySpec::LeftSideOfJoin);
        if (leftSideOfJoin) {
            auto& joinSpec = m_querySpec->GetJoinSpec();
            auto& exprs = joinSpec.equalityExprsLeft;

            // Get the indexed bucket of rows, based on the hash of the relevant expression evaluations.
            m_joinKey.clear();
            m_joinKey.reserve(exprs.size());
            XmlExprEvaluator evaluator(m_context);
            for (auto& expr : exprs) {
                m_joinKey.push_back(std::move(evaluator.Evaluate(expr)));
            }
            XmlRowHash hash(m_joinKey.size());
            size_t index = hash(m_joinKey);
            auto it = m_indexedJoin.find(index);
            
            // If there are no equality exprs, then index should be 0, and we 
            // should always get a bucket containing join rows (all of them, but that's not asserted)
            assert(exprs.size() != 0 || ((index == 0) && it != m_indexedJoin.end()));
                        
            if (it != m_indexedJoin.end()) {
                m_context->SetJoinTable(it->second);
            } 
            else if (joinSpec.outer) { 
                m_context->emptyOuterJoin = true;
            }
            else {
                return false; // no join rows found that meets equality constraints
            } 
        }
        
        while (true) {
            if (leftSideOfJoin && m_context->joinTable &&
                m_context->joinTableRowIdx == m_context->joinTable->size()) {
                break; // finished iteration of join rows
            }

            if (CheckFirstNRowsCondition()) {
                SetFlags(ParseStopped);
                break;
            }

            EvaluateNonAggregateAndSortValues(row);

            if (TestFiltersOnNonAggregateColumns()) {
                committed = true;
                if (!StoreRow(row)) {
                    if (CheckTopNRowsCondition()) {
                        SetFlags(ParseStopped);
                    }
                    else if (Streaming() && IsFlagSet(InvokeRowCallback) && m_rowCallback) {
                        m_rowCallback(0);
                    }
                }
            }
        
            if (!leftSideOfJoin || m_context->emptyOuterJoin) {
                break; // we're done
            }
            m_context->joinTableRowIdx++;
        }

        m_context->ResetJoinTable();
        return committed;
    }

    void RemoveRecycledRow()
    {
        // If the last row is marked for recycle, remove it now
        if (IsFlagSet(RecycleStorage) && m_seqRows.size()) {
            m_seqRows.erase(m_seqRows.end() - 1);
        }
        SetFlags(RecycleStorage, false);
    }

    void EvaluateNonAggregateAndSortValues(XmlRow& row)
    {
        // Evaluate all non-aggregates for storage
        XmlExprEvaluator evaluator(m_context);
        for (auto& column : GetColumns()) {
            if (column == m_querySpec->GetSortColumn()) {
                // evaluate the non-aggregate sort arguments; the resulting values go after the output values in the rows
                // (we'll evaluate the aggregates when it comes time to sort)
                size_t valueIdx = m_querySpec->GetNumValueColumns(); // advance past output values
                size_t numArgs = column->expr->GetNumArgs();
                for (size_t i = 0; i < numArgs; i++) {
                    XmlExprPtr expr = column->expr->GetArg(i);
                    if (!(expr->flags & XmlExpr::SubtreeContainsAggregate)) {
                        row[valueIdx] = std::move(evaluator.Evaluate(expr));
                    }
                    valueIdx++;
                }
            } 
            else if (column->IsPivotResult() && column->IsOutput()) {
                // Pivoter::DoPivot() wrote the pivoted values to the column expressions, 
                // so just move the value to the row.
                size_t valueIdx = column->valueIdx;
                assert(valueIdx != -1);
                row[valueIdx] = column->expr->GetValue();
            }
            else if (!column->IsAggregate() && column->IsOutput()) {
                size_t valueIdx = column->valueIdx;
                assert(valueIdx != -1);
                row[valueIdx] = std::move(evaluator.Evaluate(column->expr));
            }
        }
    }

    // Test filters operates on stored rows, after pivoting (if any) has been performed.
    // Filters on aggregated columns are handled separately
    bool TestFiltersOnNonAggregateColumns() const
    {
        XmlExprEvaluator evaluator(m_context);
        for (auto& column : GetColumns()) {
            if (!column->IsAggregate() && column->IsFilter()) {
                if (m_context->emptyOuterJoin && (column->expr->flags & XmlExpr::JoinEqualityWhere)) {
                    // we get a free pass on this filter because we're doing an outer join and
                    // are producing empty join values
                    continue; 
                }
                if (!evaluator.Evaluate(column->expr).bval) {
                    return false;
                }
            }
        }
        return true;
    }

    bool StoreRow(const XmlRow& row) // returns false if non-batched, i.e. immediate output
    {
        if (!Distinct() && !NeedsSorting()) {
            assert(m_rowRefs.size() == 0); // this will be filled in later, prior to sorting
            m_context->numRowsOutput++;
            return false;
        }

        assert(m_distinctRows.get() != nullptr);
        auto indexPos = m_distinctRows->find(row);
        bool duplicateKey = indexPos != m_distinctRows->end();

        // Store the resulting row
        size_t rowIdx;
        if (duplicateKey) {
            rowIdx = indexPos->second;
            assert(rowIdx >= 0 && rowIdx < m_rowRefs.size());
            m_rowRefs[rowIdx].second++;
        }
        else {
            rowIdx = m_rowRefs.size();
            auto it = m_distinctRows->emplace(row, rowIdx);
            XmlRow* rowRef = const_cast<XmlRow*>(&it.first->first);
            m_rowRefs.push_back(std::make_pair(rowRef, 1)); // 1 = first instance of indexed row
            if (Aggregated()) {
                m_aggregates.push_back(XmlRowAggregates(m_querySpec->GetAggrCount()));
            }
            m_context->numRowsOutput++;
        }

        if (Aggregated()) {
            // Update aggregates with new data; these values will be copied back into the
            // rows in OutputStoredRows.
            XmlExprEvaluator evaluator(m_context, &m_aggregates[rowIdx]);
            for (auto& column : GetColumns()) {
                if (column->IsAggregate()) {
                    evaluator.Evaluate(column->expr);
                }
            }
        }

        assert(m_distinctRows->size() == m_rowRefs.size());
        assert(!Aggregated() || (m_distinctRows->size() == m_aggregates.size()));

        return true;
    }

    void SortRows()
    {
        assert(NeedsSorting());

        XmlColumnPtr sortColumn = m_querySpec->GetSortColumn();
        assert(sortColumn);
        XmlExprPtr sortExpr = sortColumn->expr;
        assert(sortExpr);
        
        if (Aggregated()) {
            for (size_t rowIdx = 0; rowIdx < m_rowRefs.size(); rowIdx++) {
                XmlExprEvaluator evaluator(m_context, &m_aggregates[rowIdx]);
                XmlRow& row = *m_rowRefs[rowIdx].first;
                // evaluate the aggregate sort arguments; the resulting values go after the output values in the rows
                size_t valueIdx = m_querySpec->GetNumValueColumns(); // advance past output values
                size_t numArgs = sortExpr->GetNumArgs();
                for (size_t i = 0; i < numArgs; i++) {
                    XmlExprPtr expr = sortExpr->GetArg(i);
                    if (expr->flags & XmlExpr::SubtreeContainsAggregate) {
                        row[valueIdx] = std::move(evaluator.Evaluate(expr));
                    }
                    valueIdx++;
                }
            }
        }

        size_t firstSortValue = m_querySpec->GetNumValueColumns();
        size_t numSortValues = m_querySpec->GetNumSortValues();
        const std::vector<bool>& rev = m_querySpec->GetReversedStringSorts();

        std::sort(m_rowRefs.begin(), m_rowRefs.end(),
            [firstSortValue, numSortValues, rev](const RowRef& left, const RowRef& right) -> bool {
                const XmlValue* leftValues = &left.first->at(firstSortValue);
                const XmlValue* rightValues = &right.first->at(firstSortValue);
                for (size_t i = 0; i < numSortValues; i++) {
                    int cmp = XmlValue::Compare(leftValues[i], rightValues[i]);
                    if (cmp < 0) {
                        return !rev[i];
                    }
                    else if (cmp > 0) {
                        return rev[i];
                    }
                }
                return false;
            }
        );
    }

    const XmlColumns& GetColumns() const
    {
        return m_querySpec->GetColumns();
    }

    bool Aggregated() const
    {
        return m_querySpec->IsFlagSet(XmlQuerySpec::AggregatesExist);
    }

    bool Distinct() const
    {
        return m_querySpec->IsFlagSet(XmlQuerySpec::DistinctUsed) || Aggregated();
    }

    bool NeedsSorting() const
    {
        return !!m_querySpec->GetSortColumn() && m_querySpec->GetNumValueColumns() > 0;
    }

    bool CheckFirstNRowsCondition() const
    {
        m_context->numRowsMatched++;
        return m_querySpec->IsFlagSet(XmlQuerySpec::FirstNRowsSpecified) &&
            (m_context->numRowsMatched > m_querySpec->GetFirstNRows());
    }

    bool CheckTopNRowsCondition() const
    {
        return !NeedsSorting() && m_querySpec->IsFlagSet(XmlQuerySpec::TopNRowsSpecified) &&
            (m_context->numRowsOutput > m_querySpec->GetTopNRows());
    }

private:
    unsigned int m_flags;
    XmlParserContextPtr m_context;
    XmlQuerySpecPtr m_querySpec;
    RowCallback m_rowCallback;
    XmlRow m_joinKey;  // reusable memory used in join
    XmlRows m_seqRows; // for the non-distinct, stored rows case, if there are sort values for a row, we append those values to the end
    std::unique_ptr<DistinctRowsMap> m_distinctRows; // for the distinct, stored rows case
    std::vector<RowRef> m_rowRefs; // sortable vector with counts, points into either m_seqRows or m_distinct rows
    std::vector<XmlRowAggregates> m_aggregates;
    XmlIndexedRows m_indexedJoin; // data comes from another XmlParser instance via XmlDriver class
    XmlPivoter m_pivoter;

    XmlQuery(const XmlQuery&); // copy constructor not implemented
    const XmlQuery& operator=(const XmlQuery&); // assignment operator not implemented
};

} // namespace StreamingXml
