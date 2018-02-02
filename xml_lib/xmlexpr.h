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
#include <math.h>
#include <string>

#include "xmlop.h"

namespace StreamingXml
{
enum TokenId
{
    None,
    Id,
    StringLiteral,
    NumberLiteral,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Colon,
    Scope,
    Dot,
    // Unary tokens
    Not,
    // Binary infix tokens
    Attribute,
    FirstInfix = Attribute,
    Concat,
    Minus, // may be unary
    Plus,
    Mult,
    Div,
    Mod,
    Pow,
    And,
    Or,
    Less,
    LessEquals,
    Equals,
    NotEquals,
    GreaterEquals,
    Greater,
    LastInfix = Greater,
    // Other
    Option,
    Assign,
    Spread,
    Error,
    End
};

inline bool IsUnary(TokenId id)
{
    return id == TokenId::Not || id == TokenId::Minus;
}

inline bool IsInfix(TokenId id)
{
    return id >= FirstInfix && id <= LastInfix;
}

struct Token
{
    Token(TokenId _id, const std::string& _str)
        : id(_id)
        , str(_str)
    {
    }

    Token(const Token& other)
        : id(other.id)
        , str(other.str)
    {
    }

    Token(Token&& other)
        : id(other.id)
        , str(std::move(other.str))
    {
    }

    Token& operator=(Token&& other)
    {
        id = other.id;
        str = std::move(other.str);
        return *this;
    }

    operator std::string() const
    {
        return std::string("Token(" + str + ")");
    }

    TokenId id;
    std::string str;
};

inline bool IsBooleanLiteral(const Token& token)
{
    if (token.id != TokenId::Id) {
        return false;
    }
    std::string s(token.str);
    XmlUtils::ToLower(s);
    return s == "true" || s == "false";
}

class XmlQueryTokenizer
{
public:
    XmlQueryTokenizer(const std::string& input)
        : m_input(input)
    {
        m_pos = m_input.size() ? &m_input[0] : nullptr;
    }

    Token Lookahead(size_t lookahead = 0)
    {
        Token token = GetNext(true, lookahead);
        if (m_lookahead.size() == lookahead) {
            m_lookahead.push_back(token); // assumes we're looking ahead one-at-a-time
        }
        return std::move(token);
    }

    Token GetNext(bool lookahead = false, size_t lookaheadIndex = 0)
    {
        assert(lookahead || lookaheadIndex == 0); // !lookahead implies index is 0

        if (lookaheadIndex < m_lookahead.size()) {
            // The requested token resides in the lookahead buffer
            Token token = m_lookahead[lookaheadIndex];
            if (!lookahead) {
                m_lookahead.erase(m_lookahead.begin()); // consume the token
            }
            return std::move(token);
        }

        std::string str;

        if (!m_pos) {
            return std::move(Token(TokenId::End, str));
        }

        while (isspace(*m_pos)) {
            m_pos++;
        }

        if (*m_pos == '\0') {
            return std::move(Token(TokenId::End, str));
        }

        bool parsedDecimal = false;
        
        str += *m_pos++;
        switch (str[0]) {
            case '-':
                if (*m_pos == '-') {
                    str += *m_pos++;
                    return std::move(Token(TokenId::Option, str));
                }
                return std::move(Token(TokenId::Minus, str));
            case '+':
                return std::move(Token(TokenId::Plus, str));
            case '*':
                return std::move(Token(TokenId::Mult, str)); // this could be a wildcard; the parser decides
            case '/':
                return std::move(Token(TokenId::Div, str));
            case '%':
                return std::move(Token(TokenId::Mod, str));
            case '^':
                return std::move(Token(TokenId::Pow, str));
            case '&':
                if (*m_pos == '&') {
                    str += *m_pos++;
                    return std::move(Token(TokenId::And, str));
                }
                return std::move(Token(TokenId::Concat, str));
            case '|':
                if (*m_pos != '|') {
                    return std::move(Token(TokenId::Error, "no bitwise or"));
                }
                str += *m_pos++;
                return std::move(Token(TokenId::Or, str));
            case '(':
                return std::move(Token(TokenId::LParen, str));
            case ')':
                return std::move(Token(TokenId::RParen, str));
            case '[':
                return std::move(Token(TokenId::LBracket, str));
            case ']':
                return std::move(Token(TokenId::RBracket, str));
            case '{':
                return std::move(Token(TokenId::LBrace, str));
            case '}':
                return std::move(Token(TokenId::RBrace, str));
            case ',':
                return std::move(Token(TokenId::Comma, str));
            case '.':
                if (isdigit(*m_pos)) {
                    parsedDecimal = true;
                    goto TOKENIZE_REAL;
                }
                if (*m_pos == '.') {
                    str += *m_pos++;
                    if (*m_pos == '.') {
                        str += *m_pos++;
                        return std::move(Token(TokenId::Spread, str));
                    }
                    return std::move(Token(TokenId::Attribute, str));
                }
                return std::move(Token(TokenId::Dot, str));
            case ':':
                if (*m_pos == ':') {
                    str += *m_pos++;
                    return std::move(Token(TokenId::Scope, str));
                }
                return std::move(Token(TokenId::Colon, str));
            case '<':
                if (*m_pos == '=') {
                    str += *m_pos++;
                    return std::move(Token(TokenId::LessEquals, str));
                }
                return std::move(Token(TokenId::Less, str));
            case '=':
                if (*m_pos == '=') {
                    str += *m_pos++;
                    return std::move(Token(TokenId::Equals, str));
                }
                return std::move(Token(TokenId::Assign, str));
            case '!':
                if (*m_pos == '=') {
                    str += *m_pos++;
                    return std::move(Token(TokenId::NotEquals, str));
                }
                return std::move(Token(TokenId::Not, str));
            case '>':
                if (*m_pos == '=') {
                    str += *m_pos++;
                    return std::move(Token(TokenId::GreaterEquals, str));
                }
                return std::move(Token(TokenId::Greater, str));
            case '\"':
            case '\'': {
                char quote = str[0];
                bool escape = false;
                str.clear(); // clear open quote
                while (true) {
                    if (escape) {
                        str += *m_pos++;
                        escape = false;
                    }
                    else {
                        switch (*m_pos) {
                            case '\0':
                                return std::move(Token(TokenId::Error, "unterminated string literal"));
                            case '\\':
                                if (*++m_pos == '\0') {
                                    return std::move(Token(TokenId::Error, "dangling escape character"));
                                }
                                escape = true;
                                break; // keep parsing
                            case '\"':
                            case '\'':
                                if (*m_pos == quote) {
                                    m_pos++;
                                    return std::move(Token(TokenId::StringLiteral, str));
                                }
                            // fallthrough
                            default:
                                str += *m_pos++;
                        }
                    }
                }
                break;
            }

            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
            TOKENIZE_REAL : {
                while (isdigit(*m_pos) || (*m_pos == '.' && !parsedDecimal)) {
                    parsedDecimal |= *m_pos == '.';
                    str += *m_pos++;
                }
                if (*m_pos == '.' && parsedDecimal) {
                    str += *m_pos++;
                    return std::move(Token(TokenId::Error, str));
                }
                return std::move(Token(TokenId::NumberLiteral, str));
            }

            case '\\':
                str.pop_back();
                str += *m_pos++;
                return std::move(Token(TokenId::Error, str));

            default:
                if (isalpha(str[0]) || str[0] == '_') {
                    while (isalpha(*m_pos) || isdigit(*m_pos) || *m_pos == '_' || *m_pos == ' ') {
                        str += *m_pos++;
                    }
                    return std::move(Token(TokenId::Id, str));
                }
                return std::move(Token(TokenId::Error, str));
        }
    }

    static std::string ToString(TokenId id, const std::string& actual = std::string())
    {
        bool useActual = actual.size();
        switch (id) {
            case Id:
                return useActual ? actual : "identifier";
            case StringLiteral:
                return useActual ? actual : "string literal";
            case NumberLiteral:
                return useActual ? actual : "number literal";
            case Not:
                return "!";
            case Minus:
                return "-";
            case Plus:
                return "+";
            case Mult:
                return "*";
            case Div:
                return "/";
            case Mod:
                return "%";
            case Pow:
                return "^";
            case LParen:
                return "(";
            case RParen:
                return ")";
            case LBracket:
                return "[";
            case RBracket:
                return "]";
            case LBrace:
                return "{";
            case RBrace:
                return "}";
            case Comma:
                return ",";
            case Scope:
                return "::";
            case Colon:
                return ":";
            case Dot:
                return ".";
            case Attribute:
                return "..";
            case Less:
                return "<";
            case LessEquals:
                return "<=";
            case Equals:
                return "==";
            case NotEquals:
                return "!=";
            case GreaterEquals:
                return ">=";
            case Greater:
                return ">";
            case Option:
                return "--";
            case Assign:
                return "=";
            case Spread:
                return "...";
            case Error:
                return useActual ? actual : "error";
            case End:
                return useActual ? actual : "end of argument";
            default:
                return useActual ? actual : "???";
        }
    };

private:
    std::string m_input;
    const char* m_pos;
    std::vector<Token> m_lookahead;
};

typedef std::shared_ptr<XmlQueryTokenizer> XmlQueryTokenizerPtr;

class XmlExprTypes
{
public:
    static void InferType(XmlExprPtr expr)
    {
        XmlOperatorPtr op = expr->GetOperator();
        assert(op);

        size_t numArgs = expr->GetNumArgs();
        assert(numArgs >= op->minArgs && numArgs <= op->maxArgs);
        for (size_t i = 0; i < numArgs; i++) {
            InferType(expr->GetArg(i));
        }

        static XmlExprPtr unused(new XmlExpr);
        const XmlExprPtr arg0 = (numArgs >= 1) ? expr->GetArg(0) : unused;
        const XmlExprPtr arg1 = (numArgs >= 2) ? expr->GetArg(1) : unused;
        const XmlExprPtr arg2 = (numArgs >= 3) ? expr->GetArg(2) : unused;

        XmlType type = XmlType::Unknown;
        switch (op->opcode) {
            case Opcode::OpReal:
                expr->SetType(XmlType::Real);
                arg0->ChangeType(XmlType::Real);
                break;

            case Opcode::OpInt:
                expr->SetType(XmlType::Integer);
                arg0->ChangeType(XmlType::Integer);
                break;

            case Opcode::OpBool:
                expr->SetType(XmlType::Boolean);
                arg0->ChangeType(XmlType::Boolean);
                break;

            case Opcode::OpStr:
                expr->SetType(XmlType::String);
                if (numArgs == 1) {
                    arg0->ChangeType(XmlType::String);
                }
                else {
                    // when the precision is supplied do the type conversion explicitly in Evaluate based on the second
                    // argument
                    arg1->ChangeType(XmlType::Integer); // precision
                }
                break;

            case Opcode::OpDateTime:
                expr->SetType(XmlType::DateTime);
                arg0->ChangeType(XmlType::DateTime);
                break;

            case Opcode::OpNot:
                arg0->ChangeType(XmlType::Boolean);
                break;

            case Opcode::OpNeg:
                type = arg0->GetType();
                // For Strings, -str => str (pass through), and we use this operator to determine reverse sort order on
                // strings. For unknown types, we change the type to Real, so in order to use this feature, one must
                // use -str[value].
                type = (type == XmlType::String || type == XmlType::Integer) ? type : XmlType::Real;
                expr->SetType(type);
                arg0->ChangeType(type);
                break;

            case Opcode::OpAbs:
                type = (arg0->GetType() == XmlType::Integer) ? XmlType::Integer : XmlType::Real;
                expr->SetType(type);
                arg0->ChangeType(type);
                break;

            case Opcode::OpConcat:
                type = XmlType::String;
                expr->SetType(type);
                arg0->ChangeType(type);
                arg1->ChangeType(type);
                break;

            case Opcode::OpAdd:
            case Opcode::OpSub:
            case Opcode::OpMul:
            case Opcode::OpDiv:
                type = (arg0->GetType() == XmlType::Integer && arg1->GetType() == XmlType::Integer) ?
                    XmlType::Integer : XmlType::Real;
                expr->SetType(type);
                arg0->ChangeType(type);
                arg1->ChangeType(type);
                break;

            case Opcode::OpMod:
                type = XmlType::Integer;
                expr->SetType(type);
                arg0->ChangeType(type);
                arg1->ChangeType(type);
                break;

            case Opcode::OpOr:
            case Opcode::OpXor:
            case Opcode::OpAnd:
                type = XmlType::Boolean;
                expr->SetType(type);
                arg0->ChangeType(type);
                arg1->ChangeType(type);
                break;

            case Opcode::OpMin:
            case Opcode::OpMax:
                type = ConstrainType(arg0->GetType(), arg1->GetType());
                expr->SetType(type);
                arg0->ChangeType(type);
                arg1->ChangeType(type);
                break;

            case Opcode::OpIf:
                type = ConstrainType(arg1->GetType(), arg2->GetType());
                expr->SetType(type);
                arg0->ChangeType(XmlType::Boolean);
                arg1->ChangeType(type);
                arg2->ChangeType(type);
                break;

            case Opcode::OpSqrt:
            case Opcode::OpExp:
            case Opcode::OpLog:
                type = XmlType::Real;
                expr->SetType(type);
                arg0->ChangeType(type);
                arg1->ChangeType(type);
                break;

            case Opcode::OpPow:
                type = XmlType::Real;
                expr->SetType(type);
                arg0->ChangeType(type);
                arg1->ChangeType(type);
                break;

            case Opcode::OpFloor:
            case Opcode::OpCeil:
                type = ConstrainType(arg0->GetType(), XmlType::Real);
                expr->SetType(XmlType::Integer);
                arg0->ChangeType(ConstrainType(arg0->GetType(), type));
                break;

            case Opcode::OpLen:
                expr->SetType(XmlType::Integer);
                arg0->ChangeType(XmlType::String);
                break;

            case Opcode::OpLeft:
            case Opcode::OpRight:
                expr->SetType(XmlType::String);
                arg0->ChangeType(XmlType::String);
                arg1->ChangeType(XmlType::Integer);
                break;

            case Opcode::OpLower:
            case Opcode::OpUpper:
                expr->SetType(XmlType::String);
                arg0->ChangeType(XmlType::String);
                break;

            case Opcode::OpContains:
                expr->SetType(XmlType::Boolean);
                arg0->ChangeType(XmlType::String);
                arg1->ChangeType(XmlType::String);
                break;

            case Opcode::OpFind:
                expr->SetType(XmlType::Integer);
                arg0->ChangeType(XmlType::String);
                arg1->ChangeType(XmlType::String);
                break;

            case Opcode::OpFormatSec:
            case Opcode::OpFormatMs:
                arg0->ChangeType(XmlType::Integer);
                break;

            case Opcode::OpRound:
                type = ConstrainType(arg0->GetType(), XmlType::Real);
                expr->SetType(type);
                arg0->ChangeType(type);
                arg1->ChangeType(XmlType::Integer);
                break;

            case Opcode::OpEQ:
            case Opcode::OpNE:
            case Opcode::OpLE:
            case Opcode::OpGE:
            case Opcode::OpLT:
            case Opcode::OpGT:
                type = ConstrainType(arg0->GetType(), arg1->GetType()); // all types are represented (they're all orderable)
                expr->SetType(XmlType::Boolean);
                arg0->ChangeType(type);
                arg1->ChangeType(type);
                break;

            case Opcode::OpNodeNum: // two flavors: second arg is an numeric offset, second arg is a string to look up
                if (arg1->GetType() != XmlType::String) {
                    arg1->ChangeType(XmlType::Integer);
                }
                break;

            case Opcode::OpNodeName:
                arg1->ChangeType(XmlType::Integer);
                break;

            case Opcode::OpSum:
            case Opcode::OpMinAggr:
            case Opcode::OpMaxAggr:
                type = (arg0->GetType() == XmlType::Integer) ? XmlType::Integer : XmlType::Real;
                arg0->SetType(type);
                expr->SetType(type);
                break;

            case Opcode::OpAvg:
            case Opcode::OpStdev:
            case Opcode::OpVar:
            case Opcode::OpCov:
            case Opcode::OpCorr:
                arg0->ChangeType(XmlType::Real);
                arg1->ChangeType(XmlType::Real);
                break;

            case Opcode::OpCount:
                break; // argument type doesn't matter

            case Opcode::OpFirst:
            case Opcode::OpTop:
                arg0->ChangeType(XmlType::Integer);
                break;

            case Opcode::OpColumnRef:
                expr->SetType(XmlType::String);
                break;

            // identity functions: expression takes on type of arg
            case Opcode::OpHidden:
            case Opcode::OpWhere:
            case Opcode::OpSync:
            case Opcode::OpAny: // one of the aggregates 
                expr->SetType(arg0->GetType()); 
                break;

            case Opcode::OpCase:
            case Opcode::OpInputHeader:
            case Opcode::OpOutputHeader:
            case Opcode::OpJoinHeader:
                arg0->ChangeType(XmlType::Boolean);
                break;

            case Opcode::OpIn:
            case Opcode::OpJoin:
                arg0->ChangeType(XmlType::String);
                break;

            case Opcode::OpRoot:
                arg0->ChangeType(XmlType::Integer);
                break;

            case Opcode::OpPivot:
                arg0->ChangeType(XmlType::String);
                arg1->ChangeType(XmlType::String);
                arg2->ChangeType(XmlType::Boolean);
                break;
        }
    }
};

typedef std::vector<XmlAggregate> XmlRowAggregates; // indexed by XmlAggregateOperator::aggrIdx

class XmlExprEvaluator
{
public:
    XmlExprEvaluator(XmlParserContextPtr context, XmlRowAggregates* rowAggrs = nullptr)
        : m_context(context)
        , m_rowAggrs(rowAggrs)
    {
        if (!m_context) {
            m_context.reset(new XmlParserContext);
        }
    }

    bool WasMatched(XmlExprPtr expr) const
    {
        XmlOperatorPtr op = expr->GetOperator();
        assert(op);

        if (op->opcode == Opcode::OpPathRef) {
            auto pathRef = expr->GetPathRef();
            return !!(pathRef->flags & XmlPathRef::Matched);
        }

        size_t numArgs = expr->GetNumArgs();
        for (size_t i = 0; i < numArgs; i++) {
            if (!WasMatched(expr->GetArg(i))) {
                return false;
            }
        }
        return true;
    }

    // certain subexpressions are evaluated while parsing is occurring (i.e StartMatch and EndMatch);
    // They do not participate in the normal recursive Evaluate call.
    void ImmedEvaluate(XmlExprPtr expr)
    {
        XmlOperatorPtr op = expr->GetOperator();
        assert(op && op->flags & XmlOperator::ImmedEvaluate);
        size_t numArgs = expr->GetNumArgs();
        assert(numArgs >= op->minArgs && numArgs <= op->maxArgs);

        static XmlValue unused;
        const XmlValue& arg1 = (numArgs >= 2) ? expr->GetArg(1)->GetValue() : unused;

        switch (op->opcode) {
            case Opcode::OpPath:
            case Opcode::OpPivotPath:
            case Opcode::OpDepth:
            case Opcode::OpNodeNum:
            case Opcode::OpNodeName: 
            case Opcode::OpNodeStart:
            case Opcode::OpNodeEnd: {
                // consider the tags parsed that made the match; we want to exclude them: e.g. path(bar.baz) for
                // <foo><bar><baz> should refer to foo
                int currDepth = (int)m_context->nodeStack.size();
                // these operations are evaluated when we have matched the start tag, not the end tag
                int relativeDepth = m_context->relativeDepth - 1;
                if (currDepth < relativeDepth) {
                    // guard against the depth being less or equal than the actual tags we've parsed; leave the output
                    // empty
                    if (expr->GetType() == XmlType::Integer) {
                        expr->SetValue((__int64_t)0);
                    } 
                    else {
                        expr->SetValue(std::move(std::string()));
                    }
                    break;
                }
                int baseIdx = std::min(currDepth - relativeDepth, currDepth - 1);
                int idx;
                switch (op->opcode) {
                    case Opcode::OpPath:
                    case Opcode::OpPivotPath: {
                        std::stringstream ss;
                        for (int i = 0; i < baseIdx; i++) {
                            if (i > 0) {
                                ss << ".";
                            }
                            ss << m_context->nodeStack[i].name;
                        }
                        expr->SetValue(ss.str());
                        break;
                    }

                    case Opcode::OpDepth:
                        expr->SetValue((__int64_t)baseIdx);
                        break;

                    case Opcode::OpNodeNum: {
                        if (expr->GetNumArgs() == 1) {
                            idx = baseIdx;
                        }
                        else if (expr->GetArg(1)->GetType() == XmlType::Integer) {
                            idx = baseIdx - (int)arg1.ival; // direct indexing of ancestor
                        }
                        else {
                            // lookup ancestor by walking backward and comparing names
                            const std::string& name = arg1.sval;
                            for (idx = baseIdx; idx >= 0; idx--) {
                                if (XmlUtils::stringsEqCase(m_context->nodeStack[idx].name, name)) {
                                    break;
                                }
                            }
                        }
                        size_t result = (idx >= 0 && idx <= baseIdx) ? m_context->nodeStack[idx].nodeStart : 0;
                        expr->SetValue((__int64_t)result);
                        break;
                    }

                    case Opcode::OpNodeName:
                        if (expr->GetNumArgs() == 1) {
                            idx = baseIdx;
                        }
                        else {
                            idx = baseIdx - (int)arg1.ival;
                        }
                        if (idx >= 0 && idx <= baseIdx) {
                            expr->SetValue(m_context->nodeStack[idx].name);
                        } 
                        else {
                            expr->SetValue(std::move(std::string("")));
                        }
                        break;

                    case Opcode::OpNodeStart: 
                        expr->SetValue((__int64_t)m_context->nodeStack[baseIdx].nodeStart);
                        break;

                    case Opcode::OpNodeEnd:
                        expr->SetValue((__int64_t)m_context->numNodes); 
                        break;
                }
                break;
            }

            case Opcode::OpAttr: {
                bool found = false;
                for (int i = (int)m_context->attrStack.size() - 1; i >= 0 && !found; i--) { // bottom-up lookup
                    if (XmlUtils::stringsEqCase(m_context->attrStack[i].first, arg1.sval) &&
                        m_context->attrStack[i].second.size()) {
                        expr->SetValue(m_context->attrStack[i].second);
                        found = true;
                    }
                }
                if (!found) {
                    expr->SetValue(std::move(std::string("")));
                }
                break;
            }

            case Opcode::OpLineNum:
                expr->SetValue((__int64_t)m_context->numLines);
                break;
        }
    }

    const XmlValue& Evaluate(XmlExprPtr expr)
    {
        XmlOperatorPtr op = expr->GetOperator();
        assert(op);
        size_t numArgs = expr->GetNumArgs();
        assert(numArgs >= op->minArgs && numArgs <= op->maxArgs);

        if (op->flags & XmlOperator::ImmedEvaluate) {
            return expr->GetValue(); // already evaluated
        }

        // Bottom-up recursion
        for (size_t i = 0; i < numArgs; i++) {
            Evaluate(expr->GetArg(i));
        }

        static XmlValue unused;
        XmlValue& arg0 = (numArgs >= 1) ? expr->GetArg(0)->GetValue() : unused;
        XmlValue& arg1 = (numArgs >= 2) ? expr->GetArg(1)->GetValue() : unused;

        switch (op->opcode) {
            case Opcode::OpType:
                switch (arg0.type) {
                    case XmlType::Real:
                        expr->SetValue(std::string("real"));
                        break;
                    case XmlType::Integer:
                        expr->SetValue(std::string("int"));
                        break;
                    case XmlType::Boolean:
                        expr->SetValue(std::string("bool"));
                        break;
                    case XmlType::String:
                        expr->SetValue(std::string("str"));
                        break;
                    case XmlType::DateTime:
                        expr->SetValue(std::string("datetime"));
                        break;
                    default:
                        expr->SetValue(std::string("str")); // by convention
                        break;
                }
                break;

            case Opcode::OpColumnRef: {
                XmlColumnPtr column = expr->GetColumnRef();
                bool joinedColumn = !!(column->flags & XmlColumn::JoinedColumn);
                if (joinedColumn && m_context->emptyOuterJoin) {
                    expr->SetValue(XmlValue()); // empty value
                }
                else if (joinedColumn && m_context->joinTable) {
                    assert(m_context->joinTable);
                    const XmlRows& joinTable = *m_context->joinTable;
                    size_t rowIdx = m_context->joinTableRowIdx;
                    assert(rowIdx != -1 && rowIdx < joinTable.size());
                    size_t colIdx = column->index;
                    assert(colIdx < joinTable[rowIdx].size());
                    expr->SetValue(joinTable[rowIdx][colIdx]); 
                }
                else {
                    if (column->expr->GetOperator()->opcode == XmlOperator::OpHidden) {
                        Evaluate(column->expr); 
                    } 
                    else if (m_context->passType == XmlPassType::StoredValuesPass && 
                        column->expr->flags & XmlExpr::SubtreeContainsAggregate) {
                        // aggregate columns are recomputed on every stored row, so we need to  
                        // reevaluate the cached expression value
                        Evaluate(column->expr); 
                    }
                    // The same column reference can appear in muliple expressions with different types,
                    // so convert.
                    expr->SetValue(XmlValue::Convert(column->expr->GetValue(), expr->GetType()));
                }
                break;
            }

            case Opcode::OpPathRef:
                expr->SetValue(expr->GetPathRef()->parsedValue);
                break;

            case Opcode::OpReal:
                expr->SetValue(arg0.rval);
                break;

            case Opcode::OpInt:
                expr->SetValue(arg0.ival);
                break;

            case Opcode::OpBool:
                expr->SetValue(arg0.bval);
                break;

            case Opcode::OpStr:
                if (numArgs == 1) {
                    expr->SetValue(arg0.sval); // the type conversion was done by the assignment to the arg0 expression
                }
                else {
                    expr->SetValue(arg0.ToString(0 /*flags*/, arg1.ival /*precision*/));
                }
                break;

            case Opcode::OpDateTime:
                expr->SetValue(arg0.dtval);
                break;

            case Opcode::OpNot:
                expr->SetValue(!arg0.bval);
                break;

            case Opcode::OpNeg:
                if (arg0.type == XmlType::Unknown || arg0.type == XmlType::String) {
                    // pass-through, used to sort strings in reverse order
                    expr->SetValue(arg0.sval);
                }
                else if (arg0.type == XmlType::Integer) {
                    expr->SetValue(-arg0.ival);
                }
                else {
                    expr->SetValue(-arg0.rval);
                }
                break;

            case Opcode::OpAbs:
                if (arg0.type == XmlType::Integer) {
                    expr->SetValue(std::abs(arg0.ival));
                }
                else {
                    expr->SetValue(std::fabs(arg0.rval));
                }
                break;

            case Opcode::OpConcat:
                expr->SetValue(arg0.sval + arg1.sval);
                break;

            case Opcode::OpAdd:
                if (arg0.type == XmlType::Integer) {
                    expr->SetValue(arg0.ival + arg1.ival);
                }
                else {
                    expr->SetValue(arg0.rval + arg1.rval);
                }
                break;

            case Opcode::OpSub:
                if (arg0.type == XmlType::Integer) {
                    expr->SetValue(arg0.ival - arg1.ival);
                }
                else {
                    expr->SetValue(arg0.rval - arg1.rval);
                }
                break;

            case Opcode::OpMul:
                if (arg0.type == XmlType::Integer) {
                    expr->SetValue(arg0.ival * arg1.ival);
                }
                else {
                    expr->SetValue(arg0.rval * arg1.rval);
                }
                break;

            case Opcode::OpDiv:
                if (arg0.type == XmlType::Integer) {
                    if (arg1.ival == 0) {
                        expr->SetValue((__int64_t)0); // best we can do: no encoding for NaN
                    }
                    else {
                        expr->SetValue(arg0.ival / arg1.ival);
                    }
                }
                else if (arg1.rval == 0.0) {
                    expr->SetValue(XmlUtils::nan());
                }
                else {
                    expr->SetValue(arg0.rval / arg1.rval);
                }
                break;

            case Opcode::OpMod:
                if (arg1.ival == 0) {
                    expr->SetValue((__int64_t)-1); // Note: we do not have the ability to signal nan when we're commited
                                                   // to integer representation; -1 is a decent choice
                }
                else {
                    expr->SetValue(arg0.ival % arg1.ival);
                }
                break;

            case Opcode::OpOr:
                expr->SetValue(arg0.bval || arg1.bval);
                break;

            case Opcode::OpXor:
                expr->SetValue((bool)(arg0.bval ^ arg1.bval));
                break;

            case Opcode::OpAnd:
                expr->SetValue(arg0.bval && arg1.bval);
                break;

            case Opcode::OpMin:
                switch (arg0.type) {
                    case XmlType::Real:
                        expr->SetValue(std::min(arg0.rval, arg1.rval));
                        break;
                    case XmlType::Integer:
                        expr->SetValue(std::min(arg0.ival, arg1.ival));
                        break;
                    case XmlType::Boolean:
                        expr->SetValue(std::min(arg0.bval, arg1.bval));
                        break;
                    case XmlType::String:
                        expr->SetValue(std::min(arg0.sval, arg1.sval));
                        break;
                    case XmlType::DateTime:
                        if (arg0.dtval.ToReal() < arg1.dtval.ToReal()) {
                            expr->SetValue(arg0.dtval);
                        }
                        else {
                            expr->SetValue(arg1.dtval);
                        }
                        break;
                    default:
                        expr->SetValue(arg0.ival);
                        break;
                }
                break;

            case Opcode::OpMax:
                switch (arg0.type) {
                    case XmlType::Real:
                        expr->SetValue(std::max(arg0.rval, arg1.rval));
                        break;
                    case XmlType::Integer:
                        expr->SetValue(std::max(arg0.ival, arg1.ival));
                        break;
                    case XmlType::Boolean:
                        expr->SetValue(std::max(arg0.bval, arg1.bval));
                        break;
                    case XmlType::String:
                        expr->SetValue(std::max(arg0.sval, arg1.sval));
                        break;
                    case XmlType::DateTime:
                        if (arg0.dtval.ToReal() > arg1.dtval.ToReal()) {
                            expr->SetValue(arg0.dtval);
                        }
                        else {
                            expr->SetValue(arg1.dtval);
                        }
                        break;
                    default:
                        expr->SetValue(arg0.ival);
                        break;
                }
                break;

            case Opcode::OpIf: {
                XmlValue& arg2 = expr->GetArg(2)->GetValue();
                switch (arg1.type) {
                    case XmlType::Real:
                        expr->SetValue(arg0.bval ? arg1.rval : arg2.rval);
                        break;
                    case XmlType::Integer:
                        expr->SetValue(arg0.bval ? arg1.ival : arg2.rval);
                        break;
                    case XmlType::Boolean:
                        expr->SetValue(arg0.bval ? arg1.bval : arg2.bval);
                        break;
                    case XmlType::String:
                        expr->SetValue(arg0.bval ? arg1.sval : arg2.sval);
                        break;
                    case XmlType::DateTime:
                        expr->SetValue(arg0.bval ? arg1.dtval : arg2.dtval);
                        break;
                    default:
                        expr->SetValue(arg1.ival);
                        break;
                }
                break;
            }

            case Opcode::OpSqrt:
                expr->SetValue(std::sqrt(arg0.rval));
                break;

            case Opcode::OpLog:
                if (numArgs == 1) {
                    expr->SetValue(std::log(arg0.rval)); // natural log
                }
                else {
                    expr->SetValue(std::log(arg0.rval) / std::log(arg1.rval));
                }
                break;

            case Opcode::OpExp:
                expr->SetValue(std::exp(arg0.rval));
                break;

            case Opcode::OpPow:
                expr->SetValue(std::pow(arg0.rval, arg1.rval));
                break;

            case Opcode::OpFloor:
                if (arg0.type == XmlType::Integer) {
                    expr->SetValue(arg0.ival);
                }
                else {
                    expr->SetValue((__int64_t)std::floor(arg0.rval));
                }
                break;

            case Opcode::OpCeil:
                if (arg0.type == XmlType::Integer) {
                    expr->SetValue(arg0.ival);
                }
                else {
                    expr->SetValue((__int64_t)std::ceil(arg0.rval));
                }
                break;

            case Opcode::OpLen:
                expr->SetValue((__int64_t)arg0.sval.size());
                break;

            case Opcode::OpLeft:
                if (arg1.ival <= 0) {
                    expr->SetValue(std::string(""));
                } 
                else {
                    expr->SetValue(arg0.sval.substr(0, (size_t)arg1.ival));
                }
                break;

            case Opcode::OpRight: 
                if (arg1.ival <= 0) {
                    expr->SetValue(std::string(""));
                } else {
                    size_t len = arg0.sval.size(), take = std::min(len, (size_t)arg1.ival);
                    expr->SetValue(arg0.sval.substr(len - take, take));
                }
                break;

            case Opcode::OpLower:
                expr->SetValue(XmlUtils::ToLower(arg0.sval));
                break;

            case Opcode::OpUpper:
                expr->SetValue(XmlUtils::ToUpper(arg0.sval));
                break;

            case Opcode::OpContains:
                expr->SetValue(arg1.sval.empty() ? false : arg0.sval.find(arg1.sval) != std::string::npos);
                break;

            case Opcode::OpFind:
                expr->SetValue(arg1.sval.empty() ? -1 : (__int64_t)arg0.sval.find(arg1.sval));
                break;

            case Opcode::OpFormatSec: // given (fractional) seconds since 1970, get formatted datetime
                expr->SetValue(FormatTimestamp(arg0));
                break;

            case Opcode::OpFormatMs: // given (fractional) milliseconds since 1970, get formatted datetime
                expr->SetValue(FormatTimestamp(arg0, true));
                break;

            case Opcode::OpRound:
                if (arg0.type == XmlType::Integer) {
                    expr->SetValue(arg0.ival);
                }
                else if (arg0.rval == 0.0) {
                    expr->SetValue(0.0);
                }
                else if (arg1.ival == 0) {
                    expr->SetValue((arg0.rval > 0.0) ? std::floor(arg0.rval + 0.5) : std::ceil(arg0.rval - 0.5));
                }
                else {
                    double a = arg0.rval + ((arg0.rval < 0.0) ? -0.5 : 0.5) * std::pow(10.0, (double)-arg1.ival);
                    double p = std::pow(10, (double)arg1.ival);
                    expr->SetValue((double)((int)(a * p)) / p);
                }
                break;

            case Opcode::OpEQ:
                expr->SetValue(arg0 == arg1);
                break;

            case Opcode::OpNE:
                expr->SetValue(arg0 != arg1);
                break;

            case Opcode::OpLE:
                expr->SetValue(arg0 <= arg1);
                break;

            case Opcode::OpGE:
                expr->SetValue(arg0 >= arg1);
                break;

            case Opcode::OpLT:
                expr->SetValue(arg0 < arg1);
                break;

            case Opcode::OpGT:
                expr->SetValue(arg0 > arg1);
                break;

            case Opcode::OpRowNum:
                expr->SetValue((__int64_t)m_context->numRowsOutput + 1);
                break;

            case Opcode::OpAny:
            case Opcode::OpSum:
            case Opcode::OpMinAggr:
            case Opcode::OpMaxAggr:
            case Opcode::OpAvg:
            case Opcode::OpStdev:
            case Opcode::OpVar:
            case Opcode::OpCount:
            case Opcode::OpCov:
            case Opcode::OpCorr: {
                assert(m_rowAggrs); // for columns containing aggregate expressions, this need to be passed into the constructor
                XmlAggregateOperator* aggrOp = dynamic_cast<XmlAggregateOperator*>(op.get());
                auto& aggr = m_rowAggrs->at(aggrOp->aggrIdx);
                if (m_context->passType == XmlPassType::MainPass) {
                    if (op->opcode == Opcode::OpAny) {
                        aggr.UpdateAny(arg0);
                    }
                    else if (numArgs == 1) {
                        aggr.Update(expr->GetArg(0)->GetType() == XmlType::Integer ? arg0.ival : arg0.rval);
                    }
                    else {
                        assert(expr->GetArg(0)->GetType() == XmlType::Real && expr->GetArg(1)->GetType() == XmlType::Real);
                        aggr.Update(arg0.rval, arg1.rval);
                    }
                    expr->SetValue(0.0);
                }
                else if (m_context->passType == XmlPassType::StoredValuesPass) {
                    expr->SetValue(aggr.GetAggregate(aggrOp->aggrType));
                }
                break;
            }

            // identity functions
            case Opcode::OpWhere:
            case Opcode::OpSync:
            case Opcode::OpHidden:
                expr->SetValue(std::move(arg0)); 
                break;
        }

        return expr->GetValue();
    }

private:
    XmlParserContextPtr m_context;
    XmlRowAggregates* m_rowAggrs;
};

} // namespace StreamingXml
