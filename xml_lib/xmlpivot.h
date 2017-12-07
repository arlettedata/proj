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

namespace StreamingXml
{

class XmlPivoter
{
    enum States
    {
        StartNewPartition,
        Partitioning
    };

public:
    class Result
    {
    public:
        Result(XmlPivoter* encl)
            : encl(encl)
            , pivoted(false)
        {
        }
        
        bool WasPivoted()
        {
            return pivoted;
        }
        
        void Accept() 
        {
            encl->Accept();
        }

        bool Reject() 
        {
            return encl->Reject(*this);
        }

    private:
        bool pivoted;
        XmlPivoter* encl;
        std::vector<XmlColumnPtr> newColumns;
        friend class XmlPivoter;
    };

    XmlPivoter(XmlParserContextPtr context, IColumnEditor* columnEditor)
        : m_context(context)
        , m_columnEditor(columnEditor)
        , m_firstPass(true)
        , m_collectingColumns(true)
        , m_trainingPartitionDepth(true)
        , m_partitionDepth(0)
        , m_state(StartNewPartition)
        , m_jagged(false)
        , m_spreadIdx(-1)
    {
    }

    void BindColumns(XmlColumnPtr pivotColumn, const std::vector<std::string>& columnNames)
    {
        if (columnNames.size() == 0) {
            XmlUtils::Error("Pivot function requires column names, which can include spread (...)");
        }

        XmlExprPtr expr = pivotColumn->expr;
        if (expr->GetArg(0)->flags & XmlExpr::SubtreeContainsAggregate) {
            XmlUtils::Error("Pivot names argument must not contain aggregate functions");
        }
        if (expr->GetArg(1)->flags & XmlExpr::SubtreeContainsAggregate) {
            XmlUtils::Error("Pivot values argument must not contain aggregate functions");
        }
        if (expr->GetArg(0)->flags & XmlExpr::SubtreeContainsJoinPathRef) {
            XmlUtils::Error("Pivot names argument must not contain joined paths");
        }
        if (expr->GetArg(1)->flags & XmlExpr::SubtreeContainsJoinPathRef) {
            XmlUtils::Error("Pivot values argument must not contain joined paths");
        }
        
        m_spreadIdx = -1;

        size_t nextColumnIdx = pivotColumn->index;
        for (size_t i = 0; i < columnNames.size(); i++) {
            const std::string& colName = columnNames[i];
            if (colName == "...") {
                assert(m_spreadIdx == -1); // shouldn't be able to get here with two ... (duplicate column name error)
                m_spreadIdx = nextColumnIdx;
            }
            else {
                XmlColumnPtr column = InsertNewColumn(colName, nextColumnIdx);
                nextColumnIdx = column->index + 1;
            }
        }
        m_column = pivotColumn;
        m_columnNames = columnNames;
        m_jagged = expr->GetNumArgs() == 3 && expr->GetArg(2)->GetType() == XmlType::Boolean && expr->GetArg(2)->GetValue().bval;
    }

    bool Enabled() const
    {
        return !!m_column;
    }

    bool RequirePrepass() const
    {
        return Enabled() && m_jagged;
    }

    void Reset()
    {
        if (Enabled()) {
            m_state = StartNewPartition;
            m_collectingColumns = m_firstPass;
            m_firstPass = false;
        }
    }

    void AccumulateRow(XmlRow& row, XmlExprEvaluator& evaluator)
    {
        assert(Enabled());

        if (m_state == StartNewPartition) {
            assert(GetPartitionSize() == 0);
            if (m_trainingPartitionDepth) {
                assert(m_context->currDepth >= 0);
                m_partitionDepth = m_context->currDepth;
            }
        }

        m_state = Partitioning;
        m_names.push_back(std::move(evaluator.Evaluate(m_column->expr->GetArg(0)).sval));
        m_values.push_back(std::move(evaluator.Evaluate(m_column->expr->GetArg(1))));
    }

    size_t GetPartitionSize() const
    {
        assert(m_names.size() == m_values.size());
        return m_names.size();
    }

    Result TryPivot(std::vector<XmlRow>& rows)
    {
        Result result(this);
        
        if (!Enabled() || !IsAtEndOfPartition()) {
            return result; // with result.pivoted = false;
        }

        // Clear previous values on existing pivot columns
        std::string emptyString;
        for (auto& column : m_columnEditor->GetColumns()) {
            if (column->IsPivotResult()) {
                column->expr->SetValue(emptyString);
            }
        }

        // Gather names
        size_t partitionSize = GetPartitionSize();
        assert(partitionSize <= rows.size());
        size_t firstRowIdx = rows.size() - partitionSize;
        for (size_t idx = 0; idx < partitionSize; idx++) {
            size_t rowIdx = firstRowIdx + idx;
            const std::string& colName = m_names[idx];
            XmlColumnPtr column = m_columnEditor->GetColumn(colName);
            if (!column && m_collectingColumns && m_spreadIdx != -1) {
                column = InsertNewColumn(colName, m_spreadIdx);
                assert(m_columnEditor->GetColumn(colName) == column);
                result.newColumns.push_back(column);
                m_spreadIdx++;
            }
            if (column) {
                column->flags |= XmlColumn::PivotResultReferenced;
                // Write the pivoted value to the expression. XmlQuery will transfer the value to the stored row.
                column->expr->SetValueAndType(m_values[idx]);
            }
        }
        
        // Reset partition
        m_names.clear();
        m_values.clear();

        if (result.newColumns.size() > 0 ) {
            // Remove all the pivoted rows and create a new row of the new size
            rows.erase(rows.begin() + firstRowIdx, rows.end());
            rows.push_back(std::move(XmlRow(m_columnEditor->GetRowSize())));
        } 
        else {
            // Remove all the pivoted rows (save the first one which is recycled)
            rows.erase(rows.begin() + firstRowIdx + 1, rows.end());
        }
                   
        result.pivoted = true;
        return std::move(result);
    }

    void CheckUnreferenced() const
    {
        int cnt = 0;
        std::string colNames;
        for (auto& column : m_columnEditor->GetColumns()) {
            if (column->IsPivotResult() && !column->IsPivotResultReferenced()) {
                colNames += std::string((cnt++ ? ", " : "")) + column->name;
            }
        }
        if (cnt) {
            XmlUtils::Error("Pivot column%s not found in input: %s", cnt ? "s" : "", colNames);
        }
    }

private:
    // Called after XmlQuery passed filtering through Result::Commit()
    void Accept()
    {
        assert(Enabled());

        if (!m_jagged) {
            m_collectingColumns = false;
        }
    }

    // Called after XmlQuery failed filtering through Result::Reject()
    bool Reject(Result& result)
    {
        assert(Enabled());

        // Rollback on the new columns that were just added, since the resulting rows were all filtered out.
        for (auto column : result.newColumns) {
            m_columnEditor->DeleteColumn(column);
            if (m_spreadIdx != -1) {
                assert(m_spreadIdx > 0);
                m_spreadIdx--;
            }
        }

         // return true to recycle the row (it has the right size), false to discard the row
         return (result.newColumns.size() == 0);
    }

    bool IsAtEndOfPartition()
    {
        int currDepth = m_context->currDepth;
        if (Enabled() && m_state == Partitioning) {
            // Unless we reached the root of the input, train the depth between the first and second row.
            // (We let the depth "dip" so we can infer the depth of the group that encapsulates the rows.)
            if (GetPartitionSize() >= 2 || currDepth == 0) {
                m_trainingPartitionDepth = false;
            }
            if (m_trainingPartitionDepth) {
                m_partitionDepth = std::min(currDepth, m_partitionDepth); 
                return false;
            }
            if (currDepth < m_partitionDepth) {
                // We're leaving the min depth of the partition's name/values, so mark off a new partition
                m_state = StartNewPartition;
                return true;
            }
        }
        return false;
    }
    
    XmlColumnPtr InsertNewColumn(const std::string& colName, size_t idx = npos)
    {
        XmlExprPtr expr(new XmlExpr);
        expr->SetOperator(XmlOperatorFactory::GetInstance(Opcode::OpLiteral));
        expr->SetType(XmlType::String);
        XmlColumnPtr column(new XmlColumn(colName, expr, XmlColumn::Output | XmlColumn::PivotResult));
        m_columnEditor->InsertColumn(column, idx);
        return column;
    }

private:
    bool m_jagged;
    size_t m_spreadIdx;
    XmlColumnPtr m_column; // the column containing Pivot(), not any of its produced columns
    std::vector<std::string> m_columnNames;
    XmlParserContextPtr m_context;
    IColumnEditor* m_columnEditor;
    std::vector<std::string> m_names; // for the current partition
    std::vector<XmlValue> m_values; // for the current partition
    States m_state;
    bool m_trainingPartitionDepth;
    int m_partitionDepth;
    bool m_firstPass;
    bool m_collectingColumns;
};

} // namespace StreamingXml
