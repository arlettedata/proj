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

#include <assert.h>
#include <float.h>
#include <iomanip>
#include <limits.h>
#include <math.h>
#include <set>

#include "xmlaggr.h"

namespace StreamingXml
{

struct XmlOperator
{
    enum OpFlags
    {
        GatherData = 0x1,
        Aggregate = 0x2,
        StartMatchEval = 0x4,
        EndMatchEval = 0x8,
        ImmedEvaluate = StartMatchEval | EndMatchEval,
        OnceOnly = 0x10,
        TopLevelOnly = 0x20,
        BinaryInfix = 0x80,
        Directive = 0x100,
        NoData = 0x200,
        UnquotedStringFirstArg = 0x400,
        UnquotedStringSecondArg = 0x800,
    };

    // Note: infix operators OpNeg => OpGT are in decreasing precedences 
    // (there are no precedence classes where left- or right-associativity matters)
    enum Opcode
    {
        // clang-format off
        OpNull,
        OpColumnRef, OpPathRef, OpLiteral, // terminals
        OpNeg, OpNot, // unary -> 1-arg
        OpMul, OpDiv, OpMod, OpAdd, OpSub, OpConcat,  // 2-arg infix
        OpEQ, OpNE, OpLE, OpGE, OpLT, OpGT, //  2-arg infix 
        OpOr, OpXor, OpAnd, // 2-arg infix
        OpMin, OpMax, OpSqrt, OpPow, OpLog, OpExp, OpAbs, OpRound, OpFloor, OpCeil, // 1-arg arithmetic
        OpLen, OpContains, OpLeft, OpRight, OpUpper, OpLower, // 1-arg string
        OpFormatSec, OpFormatMs, OpRowNum, OpIf, // misc
        OpReal, OpInt, OpBool, OpStr, OpDateTime, OpType, // typing 
        OpPath, OpPivotPath, OpDepth, OpAttr, OpNodeNum, OpNodeName, OpNodeStart, OpNodeEnd, OpLineNum, // immediate functions (evaluated on path match)
        OpAny, OpSum, OpMinAggr, OpMaxAggr, OpAvg, OpStdev, OpVar, OpCov, OpCorr, OpCount, // aggregate functions
        OpFirst, OpTop, OpSort, OpPivot, OpDistinct, OpWhere, OpSync, OpRoot, OpIn, OpJoin, // directives
        OpCase, OpInputHeader, OpJoinHeader, OpOutputHeader, OpHelp // directives
        // clang-format on
    };

    XmlOperator(const std::string& name, Opcode op, size_t minArgs, size_t maxArgs, XmlType type, unsigned int flags = 0)
        : name(name)
        , opcode(op)
        , minArgs(minArgs)
        , maxArgs(maxArgs)
        , type(type)
        , flags(flags)
        , numPasses(1)
    {
        if (flags & OpFlags::Directive) {
            flags |= OpFlags::NoData;
        }
    }

    virtual ~XmlOperator() {}

    operator std::string() const
    {
        return std::string("Operator(") + name + ")";
    }

    std::string name;
    Opcode opcode;
    size_t minArgs;
    size_t maxArgs;
    XmlType type;
    unsigned int flags;
    int numPasses;
};

class XmlAggregateOperator : public XmlOperator
{
public:
    XmlAggregateOperator(XmlOperatorPtr opTemplate)
        : XmlOperator(
            opTemplate->name, 
            opTemplate->opcode, 
            opTemplate->minArgs, 
            opTemplate->maxArgs, 
            opTemplate->type,
            opTemplate->flags)
        , aggrIdx(0) // assigned by XmlColumnParser::PostprocessExprs
        , aggrType(GetAggrType(opTemplate->opcode))
    {
    }

    size_t aggrIdx;
    XmlAggrType aggrType;

    static XmlAggrType GetAggrType(Opcode opcode)
    {
        switch (opcode) {
            case XmlOperator::OpAny:
                return XmlAggrType::Any;
            case XmlOperator::OpSum:
                return XmlAggrType::Sum;
            case XmlOperator::OpAvg:
                return XmlAggrType::Avg;
            case XmlOperator::OpMinAggr:
                return XmlAggrType::Min;
            case XmlOperator::OpMaxAggr:
                return XmlAggrType::Max;
            case XmlOperator::OpVar:
                return XmlAggrType::Var;
            case XmlOperator::OpCov:
                return XmlAggrType::Cov;
            case XmlOperator::OpCorr:
                return XmlAggrType::Corr;
            case XmlOperator::OpStdev:
                return XmlAggrType::Stdev;
            case XmlOperator::OpCount:
                return XmlAggrType::Count;
            default:
                assert(false);
                return XmlAggrType::Count;
        }
    }
};

class XmlOperatorFactory
{
public:
    // Note: operator instances are pointers to support aggregate operator inheritance, where state is carried.
    // (Also, in the past, external operators implemented in Win32 DLLs were supported. That functionality was 
    // removed to simplify the code base.) 
    static XmlOperatorPtr GetInstance(XmlOperator::Opcode opcode, const std::string& name = std::string())
    {
        assert(name.empty() || (name[0] != '('));
        const size_t U = (size_t)-1;
        static XmlOperatorPtr templates[] = {
            // clang-format off
            XmlOperatorPtr(new XmlOperator( "<ColumnRef>",XmlOperator::OpColumnRef,    0, 0, XmlType::Unknown )), 
            XmlOperatorPtr(new XmlOperator( "<PathRef>",  XmlOperator::OpPathRef,      0, 0, XmlType::Unknown )), 
            XmlOperatorPtr(new XmlOperator( "<Literal>",  XmlOperator::OpLiteral,      0, 0, XmlType::Unknown )), 
        /**/XmlOperatorPtr(new XmlOperator( "case",       XmlOperator::OpCase,         0, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
        /**/XmlOperatorPtr(new XmlOperator( "help",       XmlOperator::OpHelp,         0, 0, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
/*synonym*/ XmlOperatorPtr(new XmlOperator( "usage",      XmlOperator::OpHelp,         0, 0, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),            XmlOperatorPtr(new XmlOperator( "-" ,         XmlOperator::OpNeg,          1, 1, XmlType::Real )), 
            XmlOperatorPtr(new XmlOperator( "in",         XmlOperator::OpIn,           1, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly | XmlOperator::UnquotedStringFirstArg )),
            XmlOperatorPtr(new XmlOperator( "inheader",   XmlOperator::OpInputHeader,  0, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
        /**/XmlOperatorPtr(new XmlOperator( "outheader",  XmlOperator::OpOutputHeader, 0, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
/*synonym*/ XmlOperatorPtr(new XmlOperator( "header",     XmlOperator::OpOutputHeader, 0, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
            XmlOperatorPtr(new XmlOperator( "join",       XmlOperator::OpJoin,         1, 2, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly | XmlOperator::UnquotedStringFirstArg )),
            XmlOperatorPtr(new XmlOperator( "joinheader", XmlOperator::OpJoinHeader,   0, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
            XmlOperatorPtr(new XmlOperator( "pivot",      XmlOperator::OpPivot,        2, 3, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
        /**/XmlOperatorPtr(new XmlOperator( "..",         XmlOperator::OpAttr,         2, 2, XmlType::String, XmlOperator::NoData | XmlOperator::StartMatchEval | XmlOperator::BinaryInfix )),
        /**/XmlOperatorPtr(new XmlOperator( "rownum",     XmlOperator::OpRowNum,       0, 0, XmlType::Integer )),
        /**/XmlOperatorPtr(new XmlOperator( "linenum",    XmlOperator::OpLineNum,      1, 1, XmlType::Integer,  XmlOperator::NoData | XmlOperator::StartMatchEval )),
        /**/XmlOperatorPtr(new XmlOperator( "depth",      XmlOperator::OpDepth,        1, 1, XmlType::Integer,  XmlOperator::NoData | XmlOperator::StartMatchEval )),
            XmlOperatorPtr(new XmlOperator( "sync",       XmlOperator::OpSync,         1, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly | XmlOperator::EndMatchEval )),
        /**/XmlOperatorPtr(new XmlOperator( "root",       XmlOperator::OpRoot,         1, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly | XmlOperator::UnquotedStringFirstArg )),
        /**/XmlOperatorPtr(new XmlOperator( "path",       XmlOperator::OpPath,         1, 1, XmlType::String,   XmlOperator::NoData | XmlOperator::StartMatchEval )),
        /**/XmlOperatorPtr(new XmlOperator( "pivotpath",  XmlOperator::OpPivotPath,    1, 1, XmlType::String,   XmlOperator::NoData | XmlOperator::StartMatchEval | XmlOperator::TopLevelOnly | XmlOperator::OnceOnly )), 
        /**/XmlOperatorPtr(new XmlOperator( "nodenum",    XmlOperator::OpNodeNum,      1, 2, XmlType::Integer,  XmlOperator::NoData | XmlOperator::StartMatchEval | XmlOperator::UnquotedStringSecondArg )), 
        /**/XmlOperatorPtr(new XmlOperator( "nodename",   XmlOperator::OpNodeName,     1, 2, XmlType::String,   XmlOperator::NoData | XmlOperator::StartMatchEval )),
        /**/XmlOperatorPtr(new XmlOperator( "nodestart",  XmlOperator::OpNodeStart,    1, 1, XmlType::Integer,  XmlOperator::NoData | XmlOperator::StartMatchEval | XmlOperator::UnquotedStringSecondArg )), 
        /**/XmlOperatorPtr(new XmlOperator( "nodeend",    XmlOperator::OpNodeEnd,      1, 1, XmlType::Integer,  XmlOperator::NoData | XmlOperator::EndMatchEval | XmlOperator::UnquotedStringSecondArg )), 
        /**/XmlOperatorPtr(new XmlOperator( "where",      XmlOperator::OpWhere,        1, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive )),
        /**/XmlOperatorPtr(new XmlOperator( "first",      XmlOperator::OpFirst,        1, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
        /**/XmlOperatorPtr(new XmlOperator( "top",        XmlOperator::OpTop,          1, 1, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
        /**/XmlOperatorPtr(new XmlOperator( "sort",       XmlOperator::OpSort,         1, U, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
        /**/XmlOperatorPtr(new XmlOperator( "distinct",   XmlOperator::OpDistinct,     0, 0, XmlType::Unknown,  XmlOperator::TopLevelOnly | XmlOperator::Directive | XmlOperator::OnceOnly )),
        /**/XmlOperatorPtr(new XmlOperator( "not",        XmlOperator::OpNot,          1, 1, XmlType::Boolean )), 
        /**/XmlOperatorPtr(new XmlOperator( "!",          XmlOperator::OpNot,          1, 1, XmlType::Boolean )), 
        /**/XmlOperatorPtr(new XmlOperator( "*",          XmlOperator::OpMul,          2, 2, XmlType::Real, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "/",          XmlOperator::OpDiv,          2, 2, XmlType::Real, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "%",          XmlOperator::OpMod,          2, 2, XmlType::Integer, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "+",          XmlOperator::OpAdd,          1, 2, XmlType::Real, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "-",          XmlOperator::OpSub,          2, 2, XmlType::Real, XmlOperator::BinaryInfix )),
        /**/XmlOperatorPtr(new XmlOperator( "eq",         XmlOperator::OpEQ,           2, 2, XmlType::Boolean )), 
        /**/XmlOperatorPtr(new XmlOperator( "==",         XmlOperator::OpEQ,           2, 2, XmlType::Boolean, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "ne",         XmlOperator::OpNE,           2, 2, XmlType::Boolean )), 
        /**/XmlOperatorPtr(new XmlOperator( "!=",         XmlOperator::OpNE,           2, 2, XmlType::Boolean, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "le",         XmlOperator::OpLE,           2, 2, XmlType::Boolean )), 
        /**/XmlOperatorPtr(new XmlOperator( "<=",         XmlOperator::OpLE,           2, 2, XmlType::Boolean, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "ge",         XmlOperator::OpGE,           2, 2, XmlType::Boolean )), 
        /**/XmlOperatorPtr(new XmlOperator( ">=",         XmlOperator::OpGE,           2, 2, XmlType::Boolean, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "lt",         XmlOperator::OpLT,           2, 2, XmlType::Boolean )), 
        /**/XmlOperatorPtr(new XmlOperator( "<",          XmlOperator::OpLT,           2, 2, XmlType::Boolean, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "gt",         XmlOperator::OpGT,           2, 2, XmlType::Boolean )),
        /**/XmlOperatorPtr(new XmlOperator( ">",          XmlOperator::OpGT,           2, 2, XmlType::Boolean, XmlOperator::BinaryInfix )),
        /**/XmlOperatorPtr(new XmlOperator( "and",        XmlOperator::OpAnd,          2, 2, XmlType::Boolean )),
        /**/XmlOperatorPtr(new XmlOperator( "&&",         XmlOperator::OpAnd,          2, 2, XmlType::Boolean, XmlOperator::BinaryInfix )),
        /**/XmlOperatorPtr(new XmlOperator( "or",         XmlOperator::OpOr,           2, 2, XmlType::Boolean )), 
        /**/XmlOperatorPtr(new XmlOperator( "||",         XmlOperator::OpOr,           2, 2, XmlType::Boolean, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "xor",        XmlOperator::OpXor,          2, 2, XmlType::Boolean )), 
        /**/XmlOperatorPtr(new XmlOperator( "^",          XmlOperator::OpXor,          2, 2, XmlType::Boolean, XmlOperator::BinaryInfix )), 
        /**/XmlOperatorPtr(new XmlOperator( "if",         XmlOperator::OpIf,           3, 3, XmlType::Real )),  // retyped as needed
        /**/XmlOperatorPtr(new XmlOperator( "abs",        XmlOperator::OpAbs,          1, 1, XmlType::Real )),
        /**/XmlOperatorPtr(new XmlOperator( "floor",      XmlOperator::OpFloor,        1, 1, XmlType::Real )),
        /**/XmlOperatorPtr(new XmlOperator( "ceil",       XmlOperator::OpCeil,         1, 1, XmlType::Real )),
        /**/XmlOperatorPtr(new XmlOperator( "round",      XmlOperator::OpRound,        1, 2, XmlType::Real )),
        /**/XmlOperatorPtr(new XmlOperator( "min",        XmlOperator::OpMin,          2, 2, XmlType::Real )),  // may turn into OpMinAggr 
        /**/XmlOperatorPtr(new XmlOperator( "max",        XmlOperator::OpMax,          2, 2, XmlType::Real )),  // may turn into OpMaxAggr 
        /**/XmlOperatorPtr(new XmlOperator( "sqrt",       XmlOperator::OpSqrt,         1, 1, XmlType::Real )),
        /**/XmlOperatorPtr(new XmlOperator( "pow",        XmlOperator::OpPow,          2, 2, XmlType::Real )),
        /**/XmlOperatorPtr(new XmlOperator( "log",        XmlOperator::OpLog,          1, 2, XmlType::Real )),  // default base e
        /**/XmlOperatorPtr(new XmlOperator( "exp",        XmlOperator::OpExp,          1, 1, XmlType::Real )),
        /**/XmlOperatorPtr(new XmlOperator( "&",          XmlOperator::OpConcat,       2, 2, XmlType::String, XmlOperator::BinaryInfix )), 
/*synonym*/ XmlOperatorPtr(new XmlOperator( "concat",     XmlOperator::OpConcat,       2, 2, XmlType::String )), 
        /**/XmlOperatorPtr(new XmlOperator( "len",        XmlOperator::OpLen,          1, 1, XmlType::Integer )),
        /**/XmlOperatorPtr(new XmlOperator( "left",       XmlOperator::OpLeft,         2, 2, XmlType::String )),
        /**/XmlOperatorPtr(new XmlOperator( "right",      XmlOperator::OpRight,        2, 2, XmlType::String )),
        /**/XmlOperatorPtr(new XmlOperator( "lower",      XmlOperator::OpLower,        1, 1, XmlType::String )),
        /**/XmlOperatorPtr(new XmlOperator( "upper",      XmlOperator::OpUpper,        1, 1, XmlType::String )),
        /**/XmlOperatorPtr(new XmlOperator( "contains",   XmlOperator::OpContains,     2, 2, XmlType::Boolean )),
		/**/XmlOperatorPtr(new XmlOperator( "formatsec",  XmlOperator::OpFormatSec,    1, 1, XmlType::String)),
        /**/XmlOperatorPtr(new XmlOperator( "formatms",   XmlOperator::OpFormatMs,     1, 1, XmlType::String)),
        /**/XmlOperatorPtr(new XmlOperator( "type",       XmlOperator::OpType,         1, 1, XmlType::String )),
        /**/XmlOperatorPtr(new XmlOperator( "real",       XmlOperator::OpReal,         1, 1, XmlType::Real)),
        /**/XmlOperatorPtr(new XmlOperator( "int",        XmlOperator::OpInt,          1, 1, XmlType::Integer)),
        /**/XmlOperatorPtr(new XmlOperator( "bool",       XmlOperator::OpBool,         1, 1, XmlType::Boolean)),
        /**/XmlOperatorPtr(new XmlOperator( "str",        XmlOperator::OpStr,          1, 2, XmlType::String)),
        /**/XmlOperatorPtr(new XmlOperator( "datetime",   XmlOperator::OpDateTime,     1, 1, XmlType::DateTime)),
        /**/XmlOperatorPtr(new XmlOperator( "any",        XmlOperator::OpAny,          1, 1, XmlType::String,   XmlOperator::Aggregate )),
        /**/XmlOperatorPtr(new XmlOperator( "sum",        XmlOperator::OpSum,          1, 1, XmlType::Real,     XmlOperator::Aggregate )),
        /**/XmlOperatorPtr(new XmlOperator( "avg",        XmlOperator::OpAvg,          1, 1, XmlType::Real,     XmlOperator::Aggregate )),
        /**/XmlOperatorPtr(new XmlOperator( "min",        XmlOperator::OpMinAggr,      1, 1, XmlType::Real,     XmlOperator::Aggregate )),  // special case: meaning depends on # args
        /**/XmlOperatorPtr(new XmlOperator( "max",        XmlOperator::OpMaxAggr,      1, 1, XmlType::Real,     XmlOperator::Aggregate )),
        /**/XmlOperatorPtr(new XmlOperator( "var",        XmlOperator::OpVar,          1, 1, XmlType::Real,     XmlOperator::Aggregate )),
        /**/XmlOperatorPtr(new XmlOperator( "cov",        XmlOperator::OpCov,          2, 2, XmlType::Real,     XmlOperator::Aggregate )),
        /**/XmlOperatorPtr(new XmlOperator( "corr",       XmlOperator::OpCorr,         2, 2, XmlType::Real,     XmlOperator::Aggregate )),
        /**/XmlOperatorPtr(new XmlOperator( "stdev",      XmlOperator::OpStdev,        1, 1, XmlType::Real,     XmlOperator::Aggregate )),
        /**/XmlOperatorPtr(new XmlOperator( "count",      XmlOperator::OpCount,        1, 1, XmlType::Integer,  XmlOperator::NoData | XmlOperator::Aggregate )),
            // clang-format on
        };

        XmlOperatorPtr opTemplate;
        for (size_t i = 0; i < sizeof(templates) / sizeof(templates[0]); i++) {
            size_t len = templates[i]->name.size();
            if ((opcode == templates[i]->opcode) ||
                XmlUtils::stringsEqCase(name.c_str(), templates[i]->name)) {
                opTemplate = templates[i];
                break;
            }
        }
        if (!opTemplate) {
            XmlUtils::Error("Unrecognized function: %s", name);
        }

        XmlOperatorPtr op = opTemplate;
        if (op->flags & XmlOperator::Aggregate) {
            // Turn this operator into an aggregate operator which carries more state
            op.reset(new XmlAggregateOperator(op));
        }

        return op;
    }

    static XmlOperatorPtr GetInstance(const std::string& name)
    {
        return GetInstance(XmlOperator::OpNull, name);
    }
};

typedef XmlOperator::Opcode Opcode;

} // namespace StreamingXml

// Add more template specializations for _print (see end of xmlutils.h)
template <> void _print(StreamingXml::XmlExprPtr expr)
{
    std::cout << std::string("Expr:") + (expr->GetOperator().get() 
        ? std::string(*expr->GetOperator().get()) 
        : "no-operator"
    );
}
