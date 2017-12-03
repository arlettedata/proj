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

#include <unordered_map>
#include <unordered_set>

#include "xmlexpr.h"
#include "xmlpivot.h"

namespace StreamingXml
{

class XmlQuerySpec : public IColumnEditor
{
public:
    typedef std::unordered_map<std::string, XmlPathRefPtr> XmlPathRefs;

    enum Flags
    {
        LineNumUsed = 0x1,
        GatherDataPassRequired = 0x2,
        NodeStackRequired = 0x4,
        AggregatesExist = 0x8,
        ShowUsage = 0x10,
        DistinctUsed = 0x20,
        FirstNRowsSpecified = 0x40,
        TopNRowsSpecified = 0x80,
        AttributesUsed = 0x100,
        HasPivot = 0x200,
        LeftSideOfJoin = 0x400,
        RightSideOfJoin = 0x800,
        ColumnsAdded = 0x1000,
    };

    struct InputSpec {
        InputSpec()
            : header(true)
            , scopeName("left") // default scope name for input, override with column name, e.g. foo:in[...]
        {
        }

        bool header;
        std::string filename;
        std::string scopeName;
        XmlPathRefs pathRefs; 
    };

    struct OutputSpec {
        OutputSpec() 
            : header(true)
        {
        }

        bool header; 
    };

    // JoinSpec is on behalf of the LHS of the join.  For the RHS, we create another instance
    // of this case and call AddJoinColumns instead of ParseColumnSpecs.
    struct JoinSpec {
        JoinSpec() 
            : flags(0) // flags propagated between instances of XmlQuerySpec
            , header(true)
            , outer(false) // second argument of OpJoin
            , scopeName("right") // default scope name for join
        {
        }

        unsigned int flags;
        bool header; // for csv input
        bool outer; 
        std::string filename;
        std::string scopeName;
        XmlColumns columns;
        XmlPathRefs pathRefs;
        XmlExprs equalityExprs;
    };

    XmlQuerySpec()
        : m_flags()
        , m_tokens(nullptr)
        , m_firstNRows(0)
        , m_topNRows(0)
        , m_aggrCount(0)
        , m_numValueColumns(0)
    {
    }

    const InputSpec& GetInputSpec() const
    {
        return m_inputSpec;
    }

    const OutputSpec& GetOutputSpec() const
    {
        return m_outputSpec;
    }

    const JoinSpec& GetJoinSpec() const
    {
        return m_joinSpec;
    }

    const XmlColumns& GetColumns() const
    {
        return m_columns;
    }

    XmlColumnPtr GetColumn(const std::string& colName) const
    {
        std::string colNameLower = colName;
        auto it = m_colMap.find(XmlUtils::ToLower(colNameLower));
        if (it == m_colMap.end()) {
            return nullptr;
        }
        return it->second;
    }
    
    size_t GetColumnIndex(const std::string& colName) const
    {
        XmlColumnPtr column = GetColumn(colName);
        if (!column) {
            return npos;
        }
        assert(column->index != npos);
        return column->index;
    }

    size_t GetNumValueColumns() const
    {
        return m_numValueColumns;
    }

    size_t InsertColumn(XmlColumnPtr column, size_t idx = npos)
    {
        // get insertion point
        if (idx == npos) {
            idx = m_columns.size();
        }

        // Dups should have been caught before calling this method
        assert(GetColumn(column->name) == nullptr);

        // Insert the column and update map
        column->index = idx;
        m_columns.insert(m_columns.begin() + idx, column);

        // Insert the map entry
        std::string colName = column->name;
        m_colMap.insert(std::make_pair(XmlUtils::ToLower(colName), column));
        assert(m_colMap.size() == m_columns.size());

        UpdateColumnIndices();
        return idx;
    }

    void DeleteColumn(XmlColumnPtr column) // used by pivot
    {
        size_t idx = column->index;
        if (idx == npos) {
            return;
        }

        // Delete the column
        m_columns.erase(m_columns.begin() + idx);

        // Delete the map entry
        std::string colName = column->name;
        auto it = m_colMap.find(XmlUtils::ToLower(colName));
        assert(it != m_colMap.end());
        m_colMap.erase(it);

        UpdateColumnIndices();
    }

    size_t GetAggrCount() const
    {
        return m_aggrCount;
    }

    size_t GetFirstNRows() const
    {
        return m_firstNRows;
    }

    size_t GetTopNRows() const
    {
        return m_topNRows;
    }

    size_t GetRootNodeNum() const
    {
        return m_rootNodeNum;
    }

    bool IsFlagSet(Flags flag) const
    {
        return (m_flags & flag) != 0;
    }

    const XmlColumnPtr GetSortColumn() const
    {
        return m_sortColumn;
    }

    size_t GetNumSortValues() const
    {
        return m_sortColumn ? m_sortColumn->expr->GetNumArgs() : 0;
    }

    const std::vector<bool> GetReversedStringSorts() const
    {
        return m_reversedStringSorts;
    }

    void ParseColumnSpecs(const std::vector<std::string>& columnSpecs, XmlPivoter& pivoter)
    {
        static std::string emptyName;
        static std::vector<std::string> emptyNames;

        assert(m_allColumnNames.size() == 0); // only call once

        // Remember all the names parsed, and whether they were explicit
        std::vector<std::vector<std::string>> namesPerColumn;
        std::vector<std::pair<std::string, Opcode>> overridesPerColumn;

        // First pass on column specs to get the names
        for (auto& columnSpec : columnSpecs) {
            bool explicitNames;
            // The only time more than one name is valid is on a pivot; we will check for this.
            std::vector<std::string> names = ParseColumnNames(columnSpec, explicitNames);
            for (auto& name : names) {
                // only create column references to explicitly named columns
                m_allColumnNames.push_back(explicitNames ? name : emptyName); 
            }
            namesPerColumn.push_back(explicitNames ? names : emptyNames);
            overridesPerColumn.push_back(HandleColumnNameOverrides(namesPerColumn.back()));
        }

        // Second pass to parse the column expressions and add the columns. We also make changes
        // per the overrides recorded in the first pass.
        size_t idx = 0;
        std::vector<std::string> pivotColumnNames;
        for (auto& columnSpec : columnSpecs) {
            auto& columnOverride = overridesPerColumn[idx];
            m_currentColumnNames = std::move(namesPerColumn[idx]);
            XmlColumnPtr column = ParseColumnExpr(columnSpec);
            if(!columnOverride.first.empty()) {
                column->name = columnOverride.first;
            }
            if(columnOverride.second == Opcode::OpPivot) {
                assert(m_pivotColumn);
                pivotColumnNames = m_currentColumnNames; // copy
            }
            InsertColumn(column);
            m_currentColumnNames.clear();
            idx++;
        }

        PostProcessRefs();

        if (m_pivotColumn) {
            assert(pivotColumnNames.size() > 0);
            pivoter.BindColumns(m_pivotColumn, pivotColumnNames);
        }
        
        for (auto& column : m_columns) {
            XmlExprPtr expr = column->expr;
            ValidateStructureAndHoistJoinColumns(expr);
            if (expr->flags & XmlExpr::SubtreeContainsJoinPathRef) {
                column->expr = HoistJoinExpr(expr);
            }
        }
        
        m_flags |= ColumnsAdded;
    }

    void AddJoinColumns(const JoinSpec& joinSpec)
    {
        // We assume ParseColumnSpecs has not been called for this instance of XmlQuerySpec.
        assert(m_exprs.size() == 0); 
        assert(m_inputSpec.pathRefs.size() == 0);

        if (joinSpec.columns.empty()) {
            XmlUtils::Error("Missing joined path references");
        }
        
        // Direcly add the path refs and columns as recorded into a JoinSpec by
        // XmlQuerySpec::ParseColumnSpecs/PostProcessRefs, in the main instance of XmlQuerySpec.
        m_inputSpec.pathRefs = joinSpec.pathRefs;
        for (auto& column : joinSpec.columns) {
            assert(column->flags & XmlColumn::Output && column->flags & XmlColumn::JoinedColumn);
            XmlExprPtr expr = column->expr;
            InsertColumn(column);
            assert(column->IsOutput());
            assert(column->index != -1);
        }

        m_flags |= joinSpec.flags | RightSideOfJoin | ColumnsAdded;
    }

private:
    // This is called after all the columns have been parsed, particularly because we resolve 
    // Column references
    void PostProcessRefs() {

        if (m_inputSpec.pathRefs.size() == 0) {
            if (m_flags & LeftSideOfJoin) {
                XmlUtils::Error("A join requires at least one input path reference");
            }
            if (m_sortColumn) {
                XmlUtils::Error("A sort requires at least one input path reference");
            }
            if (m_flags & DistinctUsed) {
                XmlUtils::Error("Use of distinct requires at least one input path reference");
            }
        }

        if (m_joinSpec.pathRefs.size() == 0) {
            if (m_flags & LeftSideOfJoin) {
                XmlUtils::Error("A join requires at least one joined path reference");
            }
        }

        for (auto& it : m_inputSpec.pathRefs) {
            XmlPathRefPtr pathRef = it.second;
            if (pathRef->flags & XmlPathRef::AppendData) {
                pathRef->flags &= ~XmlPathRef::NoData;
            }
        }

        for (auto& expr : m_exprs) {
            XmlOperatorPtr op = expr->GetOperator();
            if (op->flags & XmlOperator::ImmedEvaluate) {
                XmlPathRefPtr pathRef = expr->GetArg(0)->GetPathRef();
                if (!pathRef) {
                    XmlUtils::Error("First argument must be a path reference");
                }
                if (op->flags & XmlOperator::StartMatchEval) {
                    pathRef->startMatchExprs.push_back(expr);
                }
                else {
                    pathRef->endMatchExprs.push_back(expr);
                }
                if (pathRef->flags & XmlPathRef::Joined) {
                    m_joinSpec.flags |= NodeStackRequired;
                }
                else {
                    m_flags |= NodeStackRequired;
                }
            }

            Opcode opcode = expr->GetOperator()->opcode;
            switch (opcode) {
                case Opcode::OpColumnRef: {
                    // This is a temporary reference created in ParseRef, to be resolved now since 
                    // all the columns have been parsed.
                    const std::string& colName = expr->GetColumnRef()->name;
                    XmlColumnPtr column = GetColumn(colName);
                    assert(column); // we should find it
                    while (column->expr->GetColumnRef()) {
                        XmlColumnPtr nextColumn = GetColumn(column->expr->GetColumnRef()->name);
                        assert(nextColumn);
                        if (nextColumn->name == colName) {
                            XmlUtils::Error("Circular column reference: %s", colName);
                        }
                        column = nextColumn;
                    }
                    expr->SetColumnRef(column); // replace temporary column ref with the real column ref
                    break;
                }

                case Opcode::OpWhere:
                    // collect all unique joined path refs that appear on one side of a where[LHS==RHS]. 
                    // We will use this information to index the join rows for efficiency.
                    // In addition, collect the expressions on the other side side of the equality, 
                    // since we'll be using those to compute the index key from each of the input rows.
                    // To make the code more readable, these are in separate vectors rather than in a pair.
                    for (size_t eqOperand = 0; eqOperand <= 1; eqOperand++) {
                        XmlExprPtr pred = expr->GetArg(0);
                        if (pred->GetOperator()->opcode == Opcode::OpEQ) {
                            XmlExprPtr arg = pred->GetArg(eqOperand);
                            XmlColumnPtr column = arg->GetColumnRef();
                            if (column && (column->flags & XmlColumn::JoinedColumn)) {
                                column->flags |= XmlColumn::Indexed;
                                m_joinSpec.equalityExprs.push_back(pred->GetArg(1 - eqOperand)); // record other operand
                                break;
                            }
                        }
                    }
                    expr->flags |= XmlExpr::JoinEqualityWhere;
                    break;
            }
        }
    }

    void UpdateColumnIndices()
    {
        m_numValueColumns = 0;
        size_t idx = 0;
        size_t valueIdx = 0;
        for (auto& column : m_columns) {
            column->index = idx++;
            if (column->IsOutput() || column->IsAggregate()) {
                column->valueIdx = valueIdx++;
                m_numValueColumns++;
            }
            else {
                column->valueIdx = -1;
            }
        }
    }

    std::vector<std::string> ParseColumnNames(const std::string& columnSpec, bool& explicitNames)
    {
        m_tokens.reset(new XmlQueryTokenizer(columnSpec));
        explicitNames = false;
        std::vector<std::string> names;
        bool expectMoreNames = false, foundColon = false, parsingNames = true;
        do {
            std::string name;
            TokenId token = Lookahead(0).id;
            switch (token) {
                case TokenId::Id:
                case TokenId::StringLiteral:
                case TokenId::Spread:
                    name = GetExpectedNext(token).str;
                    break;
                case TokenId::LBrace:
                    GetExpectedNext(TokenId::LBrace);
                    name = ParseUnquotedString(TokenId::RBrace);
                    GetExpectedNext(TokenId::RBrace);
                    break;
            }
            if (name.empty()) {
                if (expectMoreNames) {
                    XmlUtils::Error("Expected a column name after comma");
                }
                break;
            }
            if (std::find(names.begin(), names.end(), name) != names.end()) {
                XmlUtils::Error("Duplicate column name: %s", name);
            }
            names.push_back(name);
            token = Lookahead(0).id;
            switch (token) {
                case TokenId::Comma:
                    GetExpectedNext(token);
                    explicitNames = true;
                    expectMoreNames = true;
                    break;
                case TokenId::Colon:
                    GetExpectedNext(token);
                    explicitNames = true;
                    foundColon = true;
                    expectMoreNames = false;
                    break;
                default:
                    parsingNames = false;
                    break;
            } 
        } while (expectMoreNames);

        if (!foundColon) {
            // no names were parsed (we were looking at column expression tokens), so roll back the tokenizer
            // go with a default name 
            m_tokens.reset(new XmlQueryTokenizer(columnSpec));
            explicitNames = false;
            names.clear();
            if (GetColumnIndex(columnSpec) != npos) {
                XmlUtils::Error("Duplicate column: %s", columnSpec);
            }
            names.push_back(columnSpec); // go with the default name, the full column spec
        }

        for (auto& name : names) {
            if (GetColumnIndex(name) != npos) {
                XmlUtils::Error("Duplicate column name: %s", name);
            }
        }

        return std::move(names);
    }

    XmlColumnPtr ParseColumnExpr(const std::string& columnSpec)
    {
        bool explicitNames;
        std::vector<std::string> columnNames = ParseColumnNames(columnSpec, explicitNames);
        std::string columnName = columnNames[0];

        XmlExprPtr expr(new XmlExpr);
        ParseExpr(expr);
        GetExpectedNext(TokenId::End);

        XmlColumnPtr column(new XmlColumn(columnName, expr));
        m_currentColumn = column;

        XmlExprTypes::InferType(expr);
        PostprocessColumnExprs(expr);

        m_tokens.reset();
        m_currentColumn.reset();

        return column;
    }

    void PostprocessColumnExprs(XmlExprPtr expr, int depth = 0, bool noDataParent = false)
    {
        XmlOperatorPtr op = expr->GetOperator();
        assert(op);

        if (op->flags & XmlOperator::TopLevelOnly && depth > 0) {
            XmlUtils::Error("Top-level expression only: %s", op->name);
        }

        if (op->flags & XmlOperator::OnceOnly) {
            for (auto& expr : m_exprs) {
                if (expr->GetOperator()->opcode == op->opcode) {
                    XmlUtils::Error("Expression can only be used once: %s", op->name);
                }
            }
        }

        m_exprs.push_back(expr);
        
        if (op->flags & XmlOperator::Aggregate) {
            m_currentColumn->flags |= XmlColumn::Aggregate;
            expr->flags |= XmlExpr::SubtreeContainsAggregate;
            m_flags |= AggregatesExist;
            XmlAggregateOperator* aggrOp = dynamic_cast<XmlAggregateOperator*>(op.get());
            aggrOp->aggrIdx = m_aggrCount++;
        }

        if (op->flags & XmlOperator::GatherData) {
            m_flags |= GatherDataPassRequired;
        }

        size_t numArgs = expr->GetNumArgs();
        switch (op->opcode) {
            case Opcode::OpPathRef: {
                XmlPathRefPtr pathRef = expr->GetPathRef();
                expr->flags |= (pathRef->flags & XmlPathRef::Joined)
                    ? XmlExpr::SubtreeContainsJoinPathRef
                    : XmlExpr::SubtreeContainsInputPathRef;
                if (noDataParent) {
                    pathRef->flags |= XmlPathRef::NoData;
                }
                else {
                    // used to void the NoData flag after all references are seen
                    pathRef->flags |= XmlPathRef::AppendData; 
                    pathRef->flags &= ~XmlPathRef::NoData;
                }
                break;
            }

            case Opcode::OpCase:
                if ((numArgs == 0 || expr->GetArg(0)->GetValue().bval)) {
                    XmlUtils::CaseSensitivityMode(true, true);
                }
                break;

            case Opcode::OpAttr:
                m_flags |= AttributesUsed;
                break;

            case Opcode::OpLineNum:
                m_flags |= LineNumUsed;
                break;

            case Opcode::OpDistinct:
                m_flags |= DistinctUsed;
                break;

            case Opcode::OpFirst:
                m_firstNRows = (size_t)std::max((__int64_t)0, expr->GetArg(0)->GetValue().ival);
                m_flags |= FirstNRowsSpecified;
                break;

            case Opcode::OpTop:
                m_topNRows = (size_t)std::max((__int64_t)0, expr->GetArg(0)->GetValue().ival);
                m_flags |= TopNRowsSpecified;
                break;

            case XmlOperator::OpPivot:
                m_pivotColumn = m_currentColumn;
                m_flags |= HasPivot;
                break;

            case XmlOperator::OpSort:
                m_sortColumn = m_currentColumn;
                for (size_t i = 0; i < numArgs; i++) {
                    XmlExprPtr arg = expr->GetArg(i);
                    m_reversedStringSorts.push_back(
                        (arg->GetType() == XmlType::Unknown || arg->GetType() == XmlType::String) && 
                        arg->GetOperator()->opcode == XmlOperator::OpNeg
                    );
                }
                break;

            case Opcode::OpInputHeader:
                // if header is specified, the default is true, otherwise false
                m_inputSpec.header = (numArgs == 0 || expr->GetArg(0)->GetValue().bval);
                break;

            case Opcode::OpJoinHeader:
                // if header is specified, the default is true, otherwise false
                m_joinSpec.header = (numArgs == 0 || expr->GetArg(0)->GetValue().bval);
                break;

            case Opcode::OpOutputHeader:
                // if header is specified, the default is true, otherwise false
                m_outputSpec.header = (numArgs == 0 || expr->GetArg(0)->GetValue().bval);
                break;

            case Opcode::OpHelp:
                m_flags |= ShowUsage;
                break;

            case Opcode::OpIn:
                m_inputSpec.filename = expr->GetArg(0)->GetValue().sval;
                break;

            case Opcode::OpJoin:
                m_joinSpec.filename = expr->GetArg(0)->GetValue().sval;
                if (numArgs == 2) {
                    m_joinSpec.outer = expr->GetArg(1)->GetValue().bval;
                }
                m_flags |= LeftSideOfJoin;
                break;

            case Opcode::OpSync:
                expr->GetArg(0)->GetPathRef()->flags |= XmlPathRef::Sync;
                break;

            case Opcode::OpRoot:
                m_rootNodeNum = expr->GetArg(0)->GetValue().ival;
                break;
        }

        if (depth == 0) {
            if (!(op->flags & XmlOperator::Directive)) {
                m_currentColumn->flags |= XmlColumn::Output;
            }
            if (op->opcode == Opcode::OpWhere) {
                expr->ChangeType(XmlType::Boolean);
                m_currentColumn->flags |= XmlColumn::Filter;
            }
            if (op->opcode != Opcode::OpPivot) {
                if (m_currentColumnNames.size() > 1) {
                    XmlUtils::Error("Multiple column names only valid for pivot function");
                }
                if (m_currentColumnNames.size() == 1 && m_currentColumnNames[0] == "...") {
                    XmlUtils::Error("Column name spread (...) only valid for pivot function");
                }
            }
        }

        noDataParent = (op->flags & XmlOperator::NoData) != 0;
        for( size_t i = 0; i < expr->GetNumArgs(); i++ ) {
            PostprocessColumnExprs( expr->GetArg(i), depth + 1, noDataParent );
        }
    };

    void ValidateStructureAndHoistJoinColumns(XmlExprPtr expr) 
    {
        XmlOperatorPtr op = expr->GetOperator();
        assert(op);

        if (expr->flags & XmlExpr::Visited) {
            // Column references makes the expression traversal DAG-like.
            return;
        }
        expr->flags |= XmlExpr::Visited;

        // Traverse children expressions, looking for the largest subexpressions that
        // are functions of join ref paths, not not main input ref paths, and hoist
        // them a join column.
        auto RollupFlags = [](XmlExprPtr parent, XmlExprPtr child) {
            if (child->flags & XmlExpr::SubtreeContainsAggregate) {
                if (parent->GetOperator()->flags & XmlOperator::Aggregate) {
                    XmlUtils::Error("Aggregate functions cannot be composed");
                }
                parent->flags |= XmlExpr::SubtreeContainsAggregate;
            }
            if (child->flags & XmlExpr::SubtreeContainsInputPathRef) {
                parent->flags |= XmlExpr::SubtreeContainsInputPathRef;
            }
            if (child->flags & XmlExpr::SubtreeContainsJoinPathRef) {
                // If the child is both a function of input path refs and join path refs
                // then we failed to hoist out join path-dependent expressions.
                assert(!(child->flags & XmlExpr::SubtreeContainsInputPathRef));
                parent->flags |= XmlExpr::SubtreeContainsJoinPathRef;
            }
        };

        // Traverse descendants  
        if (op->opcode == Opcode::OpColumnRef) {
            XmlExprPtr columnExpr = expr->GetColumnRef()->expr;
            ValidateStructureAndHoistJoinColumns(columnExpr);
            RollupFlags(expr, columnExpr);
        } else {
            for (size_t i = 0; i < expr->GetNumArgs(); i++) {
                XmlExprPtr arg = expr->GetArg(i);
                ValidateStructureAndHoistJoinColumns(arg); 
                RollupFlags(expr, arg);
            }
        }

        // Joined paths are to be hoisted before computing an aggregation or a function that is also
        // a function of an input path.  We can accumulate larger subtrees containing multiple
        // join path references (but no aggregations or input paths) before hoisting.
        if ((expr->flags & XmlExpr::SubtreeContainsJoinPathRef) && 
            (expr->flags & XmlExpr::SubtreeContainsInputPathRef || (op->flags & XmlOperator::Aggregate))) {
            // If an expression argument is a joined path, and involving a joined path and either an a
            // input path ref or an aggregation, hoist the join paths.
            for (size_t i = 0; i < expr->GetNumArgs(); i++) {
                XmlExprPtr arg = expr->GetArg(i);
                if (arg->flags & XmlExpr::SubtreeContainsJoinPathRef) {
                    XmlExprPtr newArg = HoistJoinExpr(arg);
                    assert(!(newArg->flags & XmlExpr::SubtreeContainsJoinPathRef));
                    expr->SetArg(i, newArg);
                }
            }
            expr->flags &= ~XmlExpr::SubtreeContainsJoinPathRef; // undo rollup of this flag
        }

        // Aggregations "erase" input path dependendies.
        if (op->flags & XmlOperator::Aggregate) {
            expr->flags &= ~XmlExpr::SubtreeContainsInputPathRef;
        }

        // Catch the case where we are trying to make a function of an input/join path and a aggregate
        // expression. e.g. foo+sum[bar].  This isn't supported. (Note literals are fine, like 1+sum[bar]).
        // Sort is exempted from this restriction, because we explicitly handle it in the query in order to
        // have arguments that are mixtures of aggregates and non-aggregates.
        if (op->opcode != XmlOperator::OpSort && 
            (expr->flags & XmlExpr::SubtreeContainsAggregate && expr->flags & XmlExpr::SubtreeContainsPathRef)) {
            XmlUtils::Error("Columns can't be functions of both aggregates and non-aggregates");
        }
    }

    XmlExprPtr HoistJoinExpr(XmlExprPtr expr) {
        assert(!(expr->flags & XmlExpr::SubtreeContainsInputPathRef));
        assert(expr->flags & XmlExpr::SubtreeContainsJoinPathRef);
        
        // Synthesize a join query column
        int columnNum = (int)m_joinSpec.columns.size() + 1;
        std::string columnName = std::string("__joincolumn_") + XmlUtils::ToString(columnNum);
        XmlColumnPtr column(new XmlColumn(columnName, expr, XmlColumn::Output | XmlColumn::JoinedColumn));
        m_joinSpec.columns.push_back(column);

        // The hoisted expression now lives under a column owned by JoinSpec, which will
        // later be used to specify a query in a different instace of XmlQuerySpec. 
        // Now produce a column ref to that column, used by the caller to replace the
        // passed-in expression.
        XmlOperatorPtr op = XmlOperatorFactory::GetInstance(Opcode::OpColumnRef);
        XmlExprPtr newExpr(new XmlExpr);
        newExpr->SetOperator(op);
        newExpr->SetType(expr->GetType());
        newExpr->SetColumnRef(column);
        return newExpr;
    }

    void DumpExpr(XmlExprPtr expr, int depth = 0)
    {
        std::string indent(depth * 2, ' ');
        switch (expr->GetOperator()->opcode) {
            case Opcode::OpColumnRef:
                std::cout << indent << "[" << expr->GetColumnRef()->name << "]";
                break;

            case Opcode::OpPathRef:
                std::cout << indent << "{" << expr->GetPathRef()->path;
                std::cout << "}";
                break;

            case Opcode::OpLiteral:
                std::cout << indent << expr->GetValue().ToString(XmlValue::QuoteStrings | XmlValue::SubsecondTimes);
                break;

            default:
                std::cout << indent << expr->GetOperator()->name;
                break;
        }
        std::cout << ":" << GetName(expr->GetType());
        std::cout << std::endl;
        for (size_t i = 0; i < expr->GetNumArgs(); i++) {
            DumpExpr(expr->GetArg(i), depth + 1);
        }
    }

    void ParseExpr(XmlExprPtr expr, XmlExprPtr parent = nullptr, bool unary = false)
    {
        std::string name;
        bool infix = false;
        bool isFirstToken = true; // for dealing with syntax error %a, /a, etc.
        TokenId lastToken = TokenId::None;
        do {
            Token tok0 = Lookahead(0);
            Token tok1 = Lookahead(1);
            switch (tok0.id) {
                case TokenId::LBrace: 
                    // Braces are used to distinguish string literals from quoted column and path references
                    ParseRef(expr);
                    break;

                case TokenId::LBracket:
                    GetExpectedNext(TokenId::LBracket);
                    ParseExpr(expr);
                    GetExpectedNext(TokenId::RBracket);
                    break;

                case TokenId::LParen:
                    GetExpectedNext(TokenId::LParen);
                    ParseExpr(expr);
                    GetExpectedNext(TokenId::RParen);
                    break;

                case TokenId::Option:
                    // Options are also funcitons: --x => f[], --x=true => x[true], --x=1,2 =>? x[1,2]
                    GetExpectedNext(TokenId::Option);
                    ParseFunctionCall(expr, TokenId::Assign, TokenId::End, true);
                    break;

                case TokenId::Id:
                    if (IsBooleanLiteral(tok0)) {
                        ParseLiteral(expr);
                    }
                    else if (tok1.id == TokenId::LParen) {
                        ParseFunctionCall(expr, TokenId::LParen, TokenId::RParen);
                    }
                    else if (tok1.id == TokenId::LBracket) {
                        ParseFunctionCall(expr, TokenId::LBracket, TokenId::RBracket);
                    }
                    else {
                        ParseRef(expr);
                    }
                    break;

                case TokenId::Not:
                    ParseUnaryOperator(expr);
                    break;

                case TokenId::Minus:
                    if (infix) {
                        ParseInfixOperator(expr, parent);
                    }
                    else {
                        ParseUnaryOperator(expr);
                    }
                    break;

                case TokenId::Mult:
                    if (infix) {
                        ParseInfixOperator(expr, parent);
                    }
                    else if (tok1.id == TokenId::Dot) {
                        ParseRef(expr);
                    }
                    else {
                        Unexpect(tok1.id, TokenId::Mult);
                    }
                    break;

                case TokenId::NumberLiteral:
                case TokenId::StringLiteral:
                    ParseLiteral(expr);
                    break;

                case TokenId::End:
                    XmlUtils::Error("Missing expression");
                    break;

                default:
                    if (tok0.id == TokenId::Error) {
                        XmlUtils::Error("Unexpected token \"%s\"", tok0.str);
                    }
                    else if (tok0.id == TokenId::Plus && isFirstToken) {
                        XmlUtils::Error("Positive operator not supported; use abs()");
                    }
                    else if (IsInfix(tok0.id) && !isFirstToken) {
                        ParseInfixOperator(expr, parent);
                    }
                    else {
                        Unexpect(tok0.id, tok0.id);
                    }
                    break;
            }
            lastToken = tok0.id;
            isFirstToken = false;
            infix = IsInfix(Lookahead(0).id);
        } while (!unary && infix);
    }

    Token Lookahead(int lookahead = 0)
    {
        return m_tokens->Lookahead(lookahead);
    }

    Token GetNext()
    {
        return m_tokens->GetNext();
    }

    Token GetExpectedNext(TokenId expectTokenId, TokenId alternative = TokenId::None)
    {
        Token token = GetNext();
        Expect(token, expectTokenId, alternative);
        return std::move(token);
    }

    static void Expect(Token token, TokenId expectTokenId, TokenId alternative = TokenId::None)
    {
        if (token.id != expectTokenId && (alternative == TokenId::None || token.id != alternative)) {
            if (alternative != TokenId::None) {
                XmlUtils::Error("Expected \"%s\" or \"%s\", got \"%s\"", XmlQueryTokenizer::ToString(expectTokenId),
                    XmlQueryTokenizer::ToString(alternative), XmlQueryTokenizer::ToString(token.id, token.str));
            }
            else {
                XmlUtils::Error("Expected \"%s\", got \"%s\"", XmlQueryTokenizer::ToString(expectTokenId),
                    XmlQueryTokenizer::ToString(token.id, token.str));
            }
        }
    }

    static void Unexpect(TokenId tokenId, TokenId unexpectedTokenId)
    {
        if (tokenId == unexpectedTokenId) {
            XmlUtils::Error("Unexpected \"%s\"", XmlQueryTokenizer::ToString(unexpectedTokenId));
        }
    }

    std::string ParseUnquotedString(TokenId endToken, TokenId alternative = TokenId::None) 
    {
        std::string str;
        while(true) {
            TokenId id = Lookahead(0).id;
            if (id == TokenId::End || id == endToken) {
                break;
            }
            if (alternative != TokenId::None && id == alternative) {
                break;
            }
            str += GetNext().str; // even error tokens
        }
        return std::move(str);
    }

    // Direct strings are strings that are not quoted.
    void ParseUnquotedString(XmlExprPtr expr, TokenId endToken, TokenId alternative = TokenId::None)
    {
        expr->SetOperator(XmlOperatorFactory::GetInstance(Opcode::OpLiteral));
        expr->SetValueAndType(std::move(ParseUnquotedString(endToken, alternative)));
    }

    void ParseLiteral(XmlExprPtr expr)
    {
        bool exactMatch;
        expr->SetOperator(XmlOperatorFactory::GetInstance(Opcode::OpLiteral));
        Token token = GetNext();
        if (token.id == TokenId::NumberLiteral) {
            expr->SetValueAndType(XmlUtils::ParseReal(token.str.c_str()));
        }
        else if ((XmlUtils::ParseBoolean(token.str.c_str(), &exactMatch) || true) && exactMatch) {
            expr->SetValueAndType(XmlUtils::ParseBoolean(token.str.c_str()));
        }
        else if (token.id == TokenId::StringLiteral) {
            expr->SetValueAndType(token.str);
        }
        else {
            Expect(token, TokenId::NumberLiteral, TokenId::StringLiteral);
        }
    }

    void ParseRef(XmlExprPtr expr)
    {
        XmlOperatorPtr op = XmlOperatorFactory::GetInstance(Opcode::OpPathRef);
        expr->SetOperator(op);
        expr->SetType(op->type);

        std::string pathSpec;
        bool joinedPathRef = false;
        while (Lookahead().id != TokenId::End) {
            if ((pathSpec.size() == 0) && Lookahead(0).id == TokenId::Id && Lookahead(1).id == TokenId::Scope) {
                // This is a scoped path reference (e.g right::ref), peel off the scope name.
                Token token = GetExpectedNext(TokenId::Id);
                GetExpectedNext(TokenId::Scope);
                if (XmlUtils::stringsEqCase(token.str, m_joinSpec.scopeName)) {
                    if (!(m_flags & LeftSideOfJoin)) {
                        XmlUtils::Error("Can't reference joined paths without a join directive"); 
                    }
                    joinedPathRef = true;
                } 
                else if (XmlUtils::stringsEqCase(token.str, m_inputSpec.scopeName)) {
                    // input scope names are provided for completeness, but don't add any information
                    // (path refs are by default assumed to refer to the main input)
                } 
                else {
                    XmlUtils::Error("Unknown scope name: %s", token.str);
                }
            }
            TokenId id = Lookahead().id;
            bool braces = id == TokenId::LBrace;
            if (braces) {
                pathSpec += GetExpectedNext(TokenId::LBrace).str;
                pathSpec += ParseUnquotedString(TokenId::RBrace);
                pathSpec += GetExpectedNext(TokenId::RBrace).str;
            } 
            else {
                if (pathSpec.size() && id == TokenId::NumberLiteral) {
                    pathSpec += GetExpectedNext(TokenId::NumberLiteral).str;
                } else {
                    pathSpec += GetExpectedNext(TokenId::Id, TokenId::Mult).str;
                }
            } 
            // If we have a token with a dot at the beginning (to include reals like .1), except
            // .. (attribute operator) or ... (spread operator), then continue parsing the ref
            if (Lookahead(0).str[0] != '.' || Lookahead(0).str[1] == '.') { 
                break;
            }
            pathSpec += GetNext().str;
        }
        
        // Check that we can split the pathSpec
        std::vector<std::string> tags(std::move(XmlUtils::Split(pathSpec, ".", "{}")));
        for(auto& tag : tags) {
            if (tag.front() == '{' && tag.back() != '}') {
                XmlUtils::Error("Unbalanced braces: %s", pathSpec); // Test with {1.2}.{3}.{\{4}.a
            }
        }

        if (!joinedPathRef && IsBindableColumnName(pathSpec)) {
            const std::string& columnName = pathSpec;
            XmlOperatorPtr op = XmlOperatorFactory::GetInstance(Opcode::OpColumnRef);
            expr->SetOperator(op);
            expr->SetType(op->type);
            // Column references are resolved after all columns have been parsed. Write down the reference with a
            // temporary column instance that has no expression.  We resolve this in PostProcessExprs.
            XmlExprPtr tempExpr(new XmlExpr);
            XmlColumnPtr temp(new XmlColumn(columnName, tempExpr));
            expr->SetColumnRef(temp);
        }
        else {
            // Record the path spec. There is only one path spec for duplicate path strings.
            XmlPathRefPtr pathRef;
            auto& pathRefs = joinedPathRef ? m_joinSpec.pathRefs : m_inputSpec.pathRefs;
            auto it = pathRefs.find(pathSpec);
            if (it != pathRefs.end()) {
                pathRef = it->second; // we've seen this ref before, reuse
            }
            else {
                // Create the PathRef used to "bind" the value that both XmlPath (the value producer) 
                // and XmlExprEvaluator (the value consumer) access.
                pathRef.reset(new XmlPathRef(pathSpec, joinedPathRef ? XmlPathRef::Joined : 0));
                pathRefs.insert(std::make_pair(pathSpec, pathRef));
            }
            expr->SetPathRef(pathRef);
        }
    }

    void ParseUnaryOperator(XmlExprPtr expr)
    {
        Token token = GetExpectedNext(TokenId::Not, TokenId::Minus);
        expr->SetOperator(XmlOperatorFactory::GetInstance((token.id == TokenId::Not) ? Opcode::OpNot : Opcode::OpNeg));
        expr->SetType(expr->GetOperator()->type);
        XmlExprPtr child(new XmlExpr);
        expr->AddArg(child);
        ParseExpr(child, expr, true /*unary*/);
    }

    void ParseInfixOperator(XmlExprPtr expr, XmlExprPtr parent)
    {
        Token token = GetNext();
        XmlOperatorPtr op = XmlOperatorFactory::GetInstance(token.str);
        if (op->opcode == Opcode::OpNeg) {
            op = XmlOperatorFactory::GetInstance(Opcode::OpSub);
        }

        // Make this the left child of a new expression.
        XmlExprPtr left(new XmlExpr(*expr));
        expr->Clear();
        expr->SetOperator(op);
        expr->SetType(op->type);
        expr->AddArg(left);

        // Now work on right child
        if (op->opcode == Opcode::OpAttr) { 
            // special case: second argument should be a string literal despite it
            // coming in as a Token::Id.
            // Next token should be an id, but we store as a string literal.
            token = GetExpectedNext(TokenId::Id);
            XmlExprPtr right(new XmlExpr);
            expr->AddArg(right);
            right->SetOperator(XmlOperatorFactory::GetInstance(Opcode::OpLiteral));
            right->SetValueAndType(token.str);
        }
        else {
            XmlExprPtr right(new XmlExpr);
            expr->AddArg(right);
            ParseExpr(right, expr);
        }

        // Parents are only given when they are binary infix operators.  If we have one, check precedence by comparing
        // opcodes, which are ordered by precedence.
        assert(!parent || (parent->GetOperator()->flags & XmlOperator::BinaryInfix));
        bool leftAssociativeFixup = parent && (parent->GetOperator()->opcode <= op->opcode); 
        if (leftAssociativeFixup) {
            // If the input was 1*2+3, leftAssociativeFixup will be true.  Currently, the AST is
            //      * <- parent
            //    1   + <- expr
            //       2 3
            //
            // This violates precedence, as 2+3 is calculated prior to multiplication.
            // Instead, rewrite as:
            //      +
            //    *   3
            //   1 2
            XmlExpr saveTop(*parent); // multiplication expression per example
            XmlExpr save2(*expr->GetArg(0)); // child 2 per example
            *parent = std::move(*expr); // moves addition expression to top, with same children 2 and 3
            *parent->GetArg(0) = saveTop; // moves multiplication expression to left child of top, overwriting child 2
            *parent->GetArg(0)->GetArg(1) = save2; // restore child 2 at the location shown in diagram
        }
    }

    void ParseFunctionCall(XmlExprPtr expr, TokenId startToken, TokenId endToken, bool startTokenOptional = true)
    {
        Token token = GetExpectedNext(TokenId::Id);

        XmlOperatorPtr op = XmlOperatorFactory::GetInstance(token.str);
        expr->SetOperator(op);
        expr->SetType(op->type);

        if (!startTokenOptional || Lookahead(0).id == startToken) {
            token = GetExpectedNext(startToken);
        }
        Unexpect(Lookahead().id, TokenId::Comma);
        if (Lookahead().id == endToken) {
            GetExpectedNext(endToken);
        }
        else {
            while (token.id != endToken && token.id != TokenId::End) {
                XmlExprPtr arg(new XmlExpr);
                expr->AddArg(arg);
                TokenId id = Lookahead(0).id;
                if ((id != TokenId::StringLiteral) && (id != TokenId::NumberLiteral) &&
                    ((expr->GetNumArgs() == 1 && op->flags & XmlOperator::OpFlags::UnquotedStringFirstArg) ||
                    (expr->GetNumArgs() == 2 && op->flags & XmlOperator::OpFlags::UnquotedStringSecondArg))) {
                        ParseUnquotedString(arg, endToken, TokenId::Comma);
                    }
                else {
                    ParseExpr(arg);
                }
                token = GetExpectedNext(TokenId::Comma, endToken);
            }
            Expect(token, endToken);
        }

        size_t numArgs = expr->GetNumArgs();
        Opcode opcode = expr->GetOperator()->opcode;

        // Deal with overloaded operators where the choice of function depends on number of arguments
        if (numArgs == 1 && opcode == Opcode::OpMin) {
            // Overload.  Change into the correct operator, which uses the same name.
            expr->SetOperator(op = XmlOperatorFactory::GetInstance(Opcode::OpMinAggr));
        }
        if (numArgs == 1 && opcode == Opcode::OpMax) {
            // Ditto
            expr->SetOperator(op = XmlOperatorFactory::GetInstance(Opcode::OpMaxAggr));
        }

        // Validate number of arguments
        if (numArgs < op->minArgs || numArgs > op->maxArgs) {
            XmlUtils::Error("Wrong number of arguments for %s", op->name);
        }
    }

    std::pair<std::string, Opcode> HandleColumnNameOverrides(const std::vector<std::string>& columnNames) 
    {
        std::string overrideName;
        Opcode opcode = Opcode::OpNull;

        // Peek for possible top-level function name (note: --function= form is not detected for sake of simplicity)
        bool isFunctionCall = Lookahead(0).id == TokenId::Id || 
            (Lookahead(1).id == TokenId::LBracket || Lookahead(1).id == TokenId::LParen);
        if (isFunctionCall) {
            try {
                opcode = XmlOperatorFactory::GetInstance(Lookahead(0).str)->opcode;
            } 
            catch(...) {
            }
            switch(opcode) {
                case Opcode::OpIn:
                    if (columnNames.size() && !columnNames[0].empty()) {
                        m_inputSpec.scopeName = columnNames[0];
                    }
                    overrideName = "__column_in";
                    break;
                case Opcode::OpJoin:
                    if (columnNames.size() && !columnNames[0].empty()) {
                        m_joinSpec.scopeName = columnNames[0];
                    }
                    overrideName = "__column_join";
                    break;
                case Opcode::OpPivot:
                    overrideName = "__column_pivot";
                    break;
            }
        }
        return std::pair<std::string, Opcode>(overrideName, opcode);
    }

    bool IsBindableColumnName(const std::string& name)
    {
        // don't bind against the current column names: we want to say [a]:a, where a is to remain a path reference
        for (auto& columnName : m_currentColumnNames ) {
            if (XmlUtils::stringsEqCase(name, columnName)) {
                return false;
            }
        }
        for (auto& columnName : m_allColumnNames ) {
            if (XmlUtils::stringsEqCase(name, columnName)) {
                return true;
            }
        }
        return false;
    }

private:
    unsigned int m_flags;
    InputSpec m_inputSpec;
    OutputSpec m_outputSpec;
    JoinSpec m_joinSpec;
    XmlColumnPtr m_currentColumn;
    std::vector<std::string> m_currentColumnNames;
    std::vector<std::string> m_allColumnNames;
    XmlQueryTokenizerPtr m_tokens;
    XmlColumns m_columns;
    std::unordered_map<std::string, XmlColumnPtr> m_colMap;
    XmlExprs m_exprs;
    XmlColumnPtr m_sortColumn;
    std::vector<bool> m_reversedStringSorts;
    XmlColumnPtr m_pivotColumn;
    size_t m_rootNodeNum;
    size_t m_firstNRows;
    size_t m_topNRows;
    size_t m_aggrCount;
    size_t m_numValueColumns;
};

typedef std::shared_ptr<XmlQuerySpec> XmlQuerySpecPtr;

} // namespace StreamingXml
