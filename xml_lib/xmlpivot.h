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
        XmlRows& rows;
        XmlRow* pivotedRow;
        std::vector<XmlColumnPtr> newColumns;
    };

public:
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

    void OnRow(XmlRow& row, XmlExprEvaluator& evaluator)
    {
        if (Enabled()) {
            if (m_state == StartNewPartition) {
                assert(GetPartitionSize() == 0);
                if (m_trainingPartitionDepth) {
                    assert(m_context->currDepth >= 0);
                    m_partitionDepth = m_context->currDepth;
                }
            }

            m_state = Partitioning;
            m_names.push_back(std::move(evaluator.Evaluate(GetNamesExpr()).sval));
            m_values.push_back(std::move(evaluator.Evaluate(GetValuesExpr())));
        }
    }

    bool OnEndTag()
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
        for (auto& column : m_columnEditor->GetColumns()) {
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
            const std::string& colName = m_names[idx];
            const XmlValue& colValue = m_values[idx];
            XmlColumnPtr column = m_columnEditor->GetColumn(colName);
            if (!column && m_collectingColumns && m_spreadIdx != -1) {
                column = InsertNewColumn(colName, m_spreadIdx);
                assert(m_columnEditor->GetColumn(colName) == column);
                context.newColumns.push_back(column);
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

        // Add the space for the current row so we can evaluate its columns
        for (auto& column : context.newColumns) {
            row.insert(row.begin() + column->valueIdx, XmlValue());
        }

        // Reset partition
        m_names.clear();
        m_values.clear();

        return std::move(context);
    }

    void CommitPivot(Context& context)
    {
        assert(Enabled());
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
        // signal caller to call RemoveRecycledRow 
        return true;
    }

    void CheckUnreferenced() const
    {
        int cnt = 0;
        std::string colNames;
        for (auto& column : m_columnEditor->GetColumns()) {
            if (column->IsPivotResult() && (m_referencedColumns.find(column) == m_referencedColumns.end())) {
                colNames += std::string((cnt++ ? ", " : "")) + column->name;
            }
        }
        if (cnt) {
            /**/XmlUtils::Error("Pivot column%s not found in input: %s", cnt ? "s" : "", colNames);
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
    XmlParserContextPtr m_context;
    IColumnEditor* m_columnEditor;
    std::vector<std::string> m_names; // for the current partition
    std::vector<XmlValue> m_values; // for the current partition
    States m_state;
    bool m_trainingPartitionDepth;
    int m_partitionDepth;
    bool m_firstPass;
    bool m_collectingColumns;
    std::unordered_set<XmlColumnPtr> m_referencedColumns;
};

} // namespace StreamingXml
