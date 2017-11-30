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

#include "xmlpath.h"
#include <unordered_set>

namespace StreamingXml
{

class XmlPivoter
{
    enum States
    {
        StartNewPartition,
        Partitioning
    };

    struct Context
    {
        Context(XmlRows& rows)
            : rows(rows)
            , pivotedRow(nullptr)
        {
        }
        std::vector<XmlColumnPtr> newColumns;
        std::vector<size_t> insertionIndices;
        XmlRow* pivotedRow;
        XmlRows& rows;
    };

public:
    XmlPivoter(IColumnEditor* columnEditor)
        : m_columnEditor(columnEditor)
        , m_collectingColumns(true)
        , m_nameValuesDepth(0)
        , m_state(StartNewPartition)
        , m_jagged(false)
        , m_spreadIdx(-1)
    {
    }

    void BindColumns(XmlColumnPtr column, const std::vector<std::string>& columnNames)
    {
        if (columnNames.size() == 0) {
            XmlUtils::Error("Pivot function requires column names, which can include spread (...)");
        }

        XmlExprPtr expr = column->expr;
        if (expr->GetArg(0)->flags & XmlExpr::SubtreeContainsAggregate) {
            XmlUtils::Error("Pivot names argument must not contain aggregate functions");
        }
        if (expr->GetArg(1)->flags & XmlExpr::SubtreeContainsAggregate) {
            XmlUtils::Error("Pivot values argument must not contain aggregate functions");
        }
        
        m_spreadIdx = -1;
        for (size_t i = 0; i < columnNames.size(); i++) {
            const std::string& colName = columnNames[i];
            if (colName == "...") {
                assert(m_spreadIdx == -1); // shouldn't be able to get here with two ... (duplicate column name error)
                m_spreadIdx = m_columnEditor->GetColumns().size();
            }
            else {
                InsertNewColumn(colName);
            }
        }
        m_column = column;
        m_columnNames = columnNames;
        m_jagged = expr->GetNumArgs() == 3 && expr->GetArg(2)->GetType() == XmlType::Boolean && 
            expr->GetArg(2)->GetValue().bval;
    }

    bool Enabled() const
    {
        return !!m_column;
    }

    bool RequirePrepass() const
    {
        return Enabled() && m_jagged;
    }

    void Reset(XmlPassType endOfPassType)
    {
        if (!Enabled()) {
            return;
        }
        m_nameValuesDepth = 0;
        m_state = StartNewPartition;
        if (endOfPassType == XmlPassType::GatherDataPass) {
            m_collectingColumns = false;
        }
    }

    void UpdatePartition(XmlRow& row, XmlExprEvaluator& evaluator)
    {
        if (!Enabled()) {
            return;
        }

        if (m_state == StartNewPartition) {
            // Walk the expression tree used to evaluate the names column. The least depth tells us
            // the "scope" of the name. We assume all other names exist at the same scope.
            // <scope><name>theName</name><value>theValue</value></scope> // distance from theName/theValue to outside
            // of scope is 3
            m_nameValuesDepth = GetLeastMatchDepth(GetNamesExpr()) - 3;
        }

        m_state = Partitioning;
        m_names.push_back(std::move(evaluator.Evaluate(GetNamesExpr()).sval));
        m_values.push_back(std::move(evaluator.Evaluate(GetValuesExpr()).sval));
    }

    bool EndPartition(int currDepth)
    {
        if (!Enabled()) {
            return false;
        }
        if (m_state != Partitioning) {
            return false;
        }
        if (currDepth < m_nameValuesDepth) {
            m_state = StartNewPartition;
            return true;
        }
        return false;
    }

    size_t GetPartitionSize() const
    {
        assert(m_names.size() == m_values.size());
        return m_names.size();
    }

    Context DoPivot(std::vector<XmlRow>& rows)
    {
        assert(Enabled());
        Context context(rows);

        // Clear previous values on existing pivot columns
        std::string emptyString;
        for (auto& column : m_columnEditor->GetColumns() ) {
            if (column->IsPivotResult()) {
                column->expr->SetValue(emptyString);
            }
        }

        // Gather names
        size_t partitionSize = GetPartitionSize();
        assert(partitionSize <= context.rows.size());
        size_t firstRowIdx = context.rows.size() - partitionSize;
        bool colsAdded = false;
        for (size_t idx = 0; idx < partitionSize; idx++) {
            size_t rowIdx = firstRowIdx + idx;
            XmlRow& row = rows[rowIdx];
            const std::string& colName = m_names[idx];
            const std::string& colValue = m_values[idx];
            XmlColumnPtr column = m_columnEditor->GetColumn(colName);
            if (!column && m_collectingColumns && m_spreadIdx != -1) {
                column = InsertNewColumn(colName, m_spreadIdx);
                context.newColumns.push_back(column);
                context.insertionIndices.push_back(m_spreadIdx);
                m_spreadIdx++;
                colsAdded = true;
            }
            if (column) {
                m_referencedColumns.insert(column);
                column->expr->SetValueAndType(colValue);
            }
        }

        // remove all the pivoted rows (save the first one which is used for the pivot result)
        context.rows.erase(context.rows.begin() + firstRowIdx + 1, context.rows.end());

        // Capture the resulting row
        XmlRow& row = context.rows.back();
        context.pivotedRow = &row;

        // Add the space for the current row so we can process it (in particular evaluate its columns)
        for (auto it = context.insertionIndices.begin(); it != context.insertionIndices.end(); it++) {
            row.insert(row.begin() + *it, XmlValue());
        }

        // Reset partition
        m_names.clear();
        m_values.clear();

        return std::move(context);
    }

    void CommitPivot(Context& context)
    {
        assert(Enabled());

        // insert gaps in the earlier rows and leave as null
        for (auto& row : context.rows) {
            for (auto idx : context.insertionIndices ) {
                row.insert(row.begin() + idx, XmlValue());
            }
        }
        if (!m_jagged) {
            m_collectingColumns = false;
        }
    }

    bool RejectPivot(Context& context)
    {
        assert(Enabled());

        // Rollback on the new columns that were just added, since the resulting rows were all filtered out.
        for (auto column : context.newColumns) {
            m_columnEditor->DeleteColumn(column);
            if (m_spreadIdx != -1) {
                assert(m_spreadIdx > 0);
                m_spreadIdx--;
            }
        }
        // signal caller to call RemoveRecycledRow if the pivoted row contains the
        // new columns, which are not obsolete
        return true;//context.newColumns.size() > 0; 
    }

    void CheckUnreferenced() const
    {
        if (!Enabled()) {
            return;
        }
        for (auto& column : m_columnEditor->GetColumns()) {
            if (column->IsPivotResult() && (m_referencedColumns.find(column) == m_referencedColumns.end())) {
                XmlUtils::Error("Pivot column not found in input: %s", column->name);
            }
        }
    }

private:
    XmlColumnPtr InsertNewColumn(const std::string& colName, size_t idx = npos)
    {
        XmlExprPtr expr(new XmlExpr);
        expr->SetOperator(XmlOperatorFactory::GetInstance(Opcode::OpLiteral));
        XmlColumnPtr column(new XmlColumn(colName, expr, XmlColumn::Output | XmlColumn::PivotResult));
        m_columnEditor->InsertColumn(column, idx);
        return column;
    }

    int GetLeastMatchDepth(XmlExprPtr expr) const // called after we made a match
    {
        int matchDepth = INT_MAX - 1; // don't want childDepth++ to overflow
        auto pathRef = expr->GetPathRef();
        if (pathRef) {
            matchDepth = pathRef->path->GetMatchDepth();
        }
        size_t numArgs = expr->GetNumArgs();
        for (size_t i = 0; i < numArgs; i++) {
            int childDepth = GetLeastMatchDepth(expr->GetArg(i));
            if (expr->GetOperator()->opcode == XmlOperator::OpAttr) {
                childDepth++; // treat <tag attr=x></tag> as if it were <tag><attr>x</attr></tag>
            }
            if (childDepth < matchDepth) {
                matchDepth = childDepth;
            }
        }
        return matchDepth;
    }

    XmlColumnPtr GetColumn() const
    {
        return m_column;
    }

    XmlExprPtr GetNamesExpr() const
    {
        // Assumes already validated and initialized
        return m_column->expr->GetArg(0);
    }

    XmlExprPtr GetValuesExpr() const
    {
        // Assumes already validated and initialized
        return m_column->expr->GetArg(1);
    }

private:
    bool m_jagged;
    size_t m_spreadIdx;
    XmlColumnPtr m_column; // the column containing Pivot(), not any of its produced columns
    std::vector<std::string> m_columnNames;
    IColumnEditor* m_columnEditor;
    std::vector<std::string> m_names; // for the current partition
    std::vector<std::string> m_values; // for the current partition
    States m_state;
    int m_nameValuesDepth;
    bool m_collectingColumns;
    std::unordered_set<XmlColumnPtr> m_referencedColumns;
};

} // namespace StreamingXml
