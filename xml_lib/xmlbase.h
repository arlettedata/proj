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

#include <functional>
#include <unordered_map>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <time.h>

#include "xmlutils.h"

#pragma GCC diagnostic ignored "-Wswitch"

namespace StreamingXml
{

class XmlExpr;
typedef std::shared_ptr<XmlExpr> XmlExprPtr;

struct XmlColumn;
typedef std::shared_ptr<XmlColumn> XmlColumnPtr;

struct XmlOperator;
typedef std::shared_ptr<XmlOperator> XmlOperatorPtr;

enum XmlType : char
{
    // The types enumerated here are ordered in a ladder: later types are convertible to earlier types.
    String,
    Real,
    Integer,
    DateTime,
    Boolean,
    Unknown
};

static const size_t npos = (size_t)-1;

// Note: XmlDateTime has no ctors since it is intended to be used in a union
struct XmlDateTime
{
    unsigned int error : 1;
    unsigned int dateonly : 1;
    unsigned int year : 14; // 4-digits
    unsigned int month : 4; // 1-12
    unsigned int day : 5; // 1-31
    unsigned int hours : 5; // 0-23
    unsigned int minutes : 6; // 0-59
    unsigned int seconds : 6; // 0-59
    unsigned int ms : 14; // 0-9999

    bool operator==(const XmlDateTime& other) const
    {
        if (error || other.error) {
            return false;
        }
        if (dateonly != other.dateonly) {
            return false;
        }
        if (year != other.year) {
            return false;
        }
        if (month != other.month) {
            return false;
        }
        if (day != other.day) {
            return false;
        }
        if (dateonly) {
            return true;
        }
        if (hours != other.hours) {
            return false;
        }
        if (minutes != other.minutes) {
            return false;
        }
        if (seconds != other.seconds) {
            return false;
        }
        if (ms != other.ms) {
            return false;
        }
        return true;
    }

    bool operator!=(const XmlDateTime& other) const
    {
        return !operator==(other);
    }

    bool operator<(const XmlDateTime& other) const
    {
        if (error || other.error) {
            return false;
        }
        if (year < other.year) {
            return true;
        }
        if (year > other.year) {
            return false;
        }
        if (month < other.month) {
            return true;
        }
        if (month > other.month) {
            return false;
        }
        if (day < other.day) {
            return true;
        }
        if (day > other.day) {
            return false;
        }
        if (dateonly && !other.dateonly) {
            return true;
        }
        if (!dateonly && other.dateonly) {
            return false;
        }
        if (dateonly && other.dateonly) {
            return false;
        }
        if (hours < other.hours) {
            return true;
        }
        if (hours > other.hours) {
            return false;
        }
        if (minutes < other.minutes) {
            return true;
        }
        if (minutes > other.minutes) {
            return false;
        }
        if (seconds < other.seconds) {
            return true;
        }
        if (seconds > other.seconds) {
            return false;
        }
        if (ms < other.ms) {
            return true;
        }
        if (ms > other.ms) {
            return false;
        }
        return false;
    }

    bool operator<=(const XmlDateTime& other) const
    {
        return operator==(other) || operator<(other);
    }

    bool operator>(const XmlDateTime& other) const
    {
        return !operator<=(other);
    }

    bool operator>=(const XmlDateTime& other) const
    {
        return !operator<(other);
    }

    time_t ToStdTime() const
    {
        struct tm tm = { 0 };
        tm.tm_year = year - 1900;
        tm.tm_mon = month - 1;
        tm.tm_mday = day;
        tm.tm_hour = hours;
        tm.tm_min = minutes;
        tm.tm_sec = seconds;
        time_t t = mktime(&tm);
        return t;
    }

    static XmlDateTime FromStdTime(time_t t)
    {
        struct tm* tm = localtime(&t); // Not threadsafe!
        XmlDateTime dt = { 0 };
        dt.year = tm->tm_year + 1900;
        dt.month = tm->tm_mon + 1;
        dt.day = tm->tm_mday;
        dt.hours = tm->tm_hour;
        dt.minutes = tm->tm_min;
        dt.seconds = tm->tm_sec;
        return dt;
    }

    // FromString expects either both date/time in the first arg, sep. by space, or across two args.
    static XmlDateTime FromString(const std::string& d_or_dt, const std::string& t = std::string())
    {
        XmlDateTime dt = { 0 };

        std::string datePart;
        std::string timePart;
        std::vector<std::string> parts = std::move(XmlUtils::Split(d_or_dt, " ")); // split date and time
        if (parts.size() == 1 && !t.empty()) {
            parts.push_back(t);
        }
        size_t numParts = parts.size();
        if (numParts == 0 || numParts > 2) {
            dt.error = true;
            return std::move(dt);
        }

        if (numParts == 1) {
            if (parts[0].find('-') != std::string::npos) { // do we have a date or time?
                datePart = std::move(parts[0]);
            }
            else {
                timePart = std::move(parts[0]);
            }
        }
        else {
            datePart = parts[0];
            timePart = parts[1];
        }

        bool dateOk = false;
        if (datePart.empty()) {
            dateOk = true;
        }
        else {
            parts = std::move(XmlUtils::Split(datePart, "-"));
            if (parts.size() == 3) {
                dt.year = atoi(parts[0].c_str());
                if (dt.year >= 0 && dt.year <= 49) {
                    dt.year += 2000;
                }
                else if (dt.year >= 0 && dt.year <= 99) {
                    dt.year += 1900;
                }
                dt.month = atoi(parts[1].c_str());
                dt.day = atoi(parts[2].c_str());
                dateOk = dt.year >= 0 && dt.month >= 1 && dt.month <= 12 && dt.day >= 1
                    && dt.day <= 31; // Note: not fully validated
            }
        }

        bool timeOk = false;
        if (timePart.empty()) {
            dt.dateonly = true;
            timeOk = true;
        }
        else {
            parts = std::move(XmlUtils::Split(timePart, ":"));
            if (parts.size() >= 3) {
                dt.hours = atoi(parts[0].c_str());
                dt.minutes = atoi(parts[1].c_str());
                std::string msPart;
                if (parts.size() >= 4) {
                    // treat sec and ms as separated by :
                    dt.seconds = atoi(parts[2].c_str());
                    msPart = std::move(parts[3]);
                } 
                else {
                    // sec and possible ms separated by .
                    std::vector<std::string> s_ms = std::move(XmlUtils::Split(parts[2], ".")); 
                    dt.seconds = atoi(s_ms[0].c_str());
                    if (s_ms.size() >= 2) {
                        msPart = std::move(s_ms[1]);
                    }
                }
                
                if (!msPart.empty()) {
                    // Suppose sec.ms is 8.12345678. We want 1235 to be the ms component.
                    int ms;
                    char* frac = (char*)msPart.c_str();
                    if (strlen(frac) < 5) {
                        ms = atoi(frac);
                    }
                    else {
                        int r = frac[4] + '0';
                        frac[4] = '\0';
                        ms = atoi(frac);
                        if (r >= 5) {
                            ms++;
                        }
                    }
                    dt.ms = ms;
                }
                timeOk = dt.hours >= 0 && dt.hours < 24 && dt.minutes >= 0 && dt.minutes < 60 && dt.seconds >= 0 &&
                    dt.seconds < 60 && dt.ms >= 0 && dt.ms < 10000;
                if ((parts[2].find("pm") != std::string::npos || parts[2].find("PM") != std::string::npos) && dt.hours < 12) {
                    dt.hours += 12;
                }
            }
        }

        dt.error = !dateOk || !timeOk;
        return std::move(dt);
    }

    static XmlDateTime FromReal(double d) // This is a lossy conversion
    {
        __int64_t i = (__int64_t)d;
        XmlDateTime dt = FromInteger(i);
        dt.ms = (int)((d - i) * 10000.00);
        return dt;
    }

    static XmlDateTime FromInteger(__int64_t i)
    {
        return FromStdTime((time_t)i);
    }

    // This is a lossy conversion
    double ToReal() const
    {
        time_t t = ToStdTime();
        double d = t + (ms / 10000.0);
        return d;
    }

    __int64_t ToInteger() const
    {
        time_t t = ToStdTime();
        return (__int64_t)t;
    }

    std::string ToString(bool subsecondTimes = true) const
    {
        std::stringstream ss;
        ss << std::setfill('0');
        if (!error) {
            ss << std::setw(4) << year << "-" << std::setw(2) << month << "-" << std::setw(2) << day;
            if (!dateonly) {
                ss << " " << std::setw(2) << hours << ":" << std::setw(2) << minutes << ":" << std::setw(2) << seconds;
                if (subsecondTimes) {
                    ss << "." << ms;
                }
            }
        }
        return std::move(ss.str());
    }
};

inline XmlType InferType(const char* s)
{
    while (isspace(*s)) {
        s++;
    }

    if (*s == '\0') {
        return XmlType::String;
    }

    // Start with most restrictive format
    if (!XmlDateTime::FromString(s).error) {
        return XmlType::DateTime;
    }

    bool exactMatch;
    XmlUtils::ParseBoolean(s, &exactMatch);
    if (exactMatch) {
        return XmlType::Boolean;
    }

    XmlUtils::ParseInteger(s, &exactMatch);
    if (exactMatch) {
        return XmlType::Integer;
    }

    XmlUtils::ParseReal(s, &exactMatch);
    if (exactMatch) {
        return XmlType::Real;
    }

    return XmlType::String;
}

inline XmlType ParseDataType(const char* s)
{
    assert(s);
    switch (tolower(*s)) {
        case 'r':
            return XmlType::Real;
        case 'i':
            return XmlType::Integer;
        case 'b':
            return XmlType::Boolean;
        case 's':
            return XmlType::String;
        case 'd':
            return XmlType::DateTime; // can also be time-only
        default:
            return XmlType::Unknown;
    }
}

inline std::string GetName(XmlType type)
{
    switch (type) {
        case XmlType::Integer:
            return "Integer";
        case XmlType::Real:
            return "Real";
        case XmlType::Boolean:
            return "Boolean";
        case XmlType::String:
            return "String";
        case XmlType::DateTime:
            return "DateTime";
        default:
            return "Unknown";
    }
}

const int DEFAULT_PRECISION = 10;

inline XmlType ConstrainType(XmlType t1, XmlType t2, XmlType t3 = XmlType::Unknown)
{
    return std::min(std::min(t1, t2), t3);
}

struct XmlValue
{
    XmlType type;
    std::string sval;
    union {
        double rval;
        bool bval;
        __int64_t ival;
        XmlDateTime dtval;
    };

    XmlValue()
        : type(XmlType::Unknown)
        , ival(0)
    {
    }

    XmlValue(const XmlValue& other)
    {
        type = other.type;
        ival = other.ival;
        sval = other.sval;
    }

    XmlValue(XmlValue&& other)
    {
        type = other.type;
        ival = other.ival;
        sval = std::move(other.sval);
    }

    XmlValue& operator=(const XmlValue& other)
    {
        type = other.type;
        ival = other.ival;
        sval = other.sval;
        return *this;
    }

    XmlValue& operator=(XmlValue&& other)
    {
        type = other.type;
        ival = other.ival;
        sval = std::move(other.sval);
        return *this;
    }

    static int Compare(const XmlValue& v1, const XmlValue& v2)
    {
        if (v1.type != v2.type) {
            return (v1.type < v2.type) ? -1 : (v1.type == v2.type) ? 0 : 1;
        }

        switch (v1.type) {
            case XmlType::Real:
                return (v1.rval < v2.rval) ? -1 : (v1.rval == v2.rval) ? 0 : 1;
            case XmlType::Integer:
                return (v1.ival < v2.ival) ? -1 : (v1.ival == v2.ival) ? 0 : 1;
            case XmlType::Boolean:
                return (v1.bval < v2.bval) ? -1 : (v1.bval == v2.bval) ? 0 : 1;
            case XmlType::DateTime:
                return (v1.dtval < v2.dtval) ? -1 : (v1.dtval == v2.dtval) ? 0 : 1;
            case XmlType::String:
                return strcmp(v1.sval.c_str(), v2.sval.c_str());
            default:
                return 0;
        }
    }

    bool operator==(const XmlValue& other) const
    {
        return Compare(*this, other) == 0;
    }

    bool operator<=(const XmlValue& other) const
    {
        return Compare(*this, other) <= 0;
    }

    bool operator<(const XmlValue& other) const
    {
        return Compare(*this, other) < 0;
    }

    bool operator!=(const XmlValue& other) const
    {
        return Compare(*this, other) != 0;
    }

    bool operator>(const XmlValue& other) const
    {
        return Compare(*this, other) > 0;
    }

    bool operator>=(const XmlValue& other) const
    {
        return Compare(*this, other) >= 0;
    }

    XmlValue(__int64_t ival)
        : type(XmlType::Integer)
        , ival(ival)
    {
    }

    XmlValue(double rval)
        : type(XmlType::Real)
        , rval(rval)
    {
    }

    XmlValue(bool _bval)
        : type(XmlType::Boolean)
        , ival(0)
    {
        bval = _bval;
    }

    XmlValue(const std::string& sval)
        : type(XmlType::String)
        , sval(sval)
        , ival(0)
    {
    }

    XmlValue(const XmlDateTime& dtval)
        : type(XmlType::DateTime)
        , dtval(dtval)
    {
    }

    enum Flags
    {
        QuoteStrings = 0x1,
        SubsecondTimes = 0x2
    };

    operator std::string() const
    {
        return std::string("Value(") + ToString() + ":" + GetName(type) + ")";
    }

    std::string ToString(unsigned int flags = 0, int precision = DEFAULT_PRECISION) const
    {
        switch (type) {
            case XmlType::String: {
                std::stringstream ss;
                if (flags & QuoteStrings) {
                    ss << "\"";
                }
                ss << sval; // XmlUtils::UnescapeCharacters(sval);
                if (flags & QuoteStrings) {
                    ss << "\"";
                }
                return std::move(ss.str());
            }

            case XmlType::Integer:
                return std::move(XmlUtils::ToString(ival));

            case XmlType::Real:
                return std::move(XmlUtils::ToString(rval, precision));

            case XmlType::Boolean:
                return std::move(XmlUtils::ToString(bval));

            case XmlType::DateTime:
                return std::move(dtval.ToString(!!(flags & SubsecondTimes)));

            default:
                return "";
        }
    }

    static XmlValue Convert(const XmlValue& fromValue, XmlType toType)
    {
        // Note all returns use implict XmlValue construction
        switch (toType) {
            case XmlType::Real:
                switch (fromValue.type) {
                    case XmlType::Real:
                        return fromValue.rval;
                    case XmlType::Integer:
                        return double(fromValue.ival);
                    case XmlType::Boolean:
                        return double(fromValue.bval);
                    case XmlType::String:
                        return XmlUtils::ParseReal(fromValue.sval.c_str());
                    case XmlType::DateTime:
                        return fromValue.dtval.ToReal(); // Note: lossy conversion
                    default:
                        return 0.0;
                }
                break;

            case XmlType::Integer:
                switch (fromValue.type) {
                    case XmlType::Real:
                        return __int64_t(fromValue.rval);
                    case XmlType::Integer:
                        return fromValue.ival;
                    case XmlType::Boolean:
                        return __int64_t(fromValue.bval);
                    case XmlType::String:
                        return XmlUtils::ParseInteger(fromValue.sval.c_str());
                    case XmlType::DateTime:
                        return fromValue.dtval.ToInteger();
                    default:
                        return __int64_t(0);
                }
                break;

            case XmlType::Boolean:
                switch (fromValue.type) {
                    case XmlType::Real:
                        return fromValue.rval != 0.0;
                    case XmlType::Integer:
                        return fromValue.ival != 0;
                    case XmlType::Boolean:
                        return fromValue.bval;
                    case XmlType::String:
                        return fromValue.sval.size() && fromValue.sval != "false" && fromValue.sval[0] != '0';
                    case XmlType::DateTime:
                        return false; // no conversion makes sense
                    default:
                        return false;
                }
                break;

            case XmlType::String:
            case XmlType::Unknown:
                switch (fromValue.type) {
                    case XmlType::Real:
                        return std::move(XmlUtils::ToString(fromValue.rval));
                    case XmlType::Integer:
                        return std::move(XmlUtils::ToString(fromValue.ival));
                    case XmlType::Boolean:
                        return std::move(XmlUtils::ToString(fromValue.bval));
                    case XmlType::String:
                        return fromValue.sval;
                    case XmlType::DateTime:
                        return std::move(fromValue.dtval.ToString());
                    default:
                        return std::move(std::string());
                }
                break;

            case XmlType::DateTime:
                switch (fromValue.type) {
                    case XmlType::Real:
                        return XmlDateTime::FromReal(fromValue.rval); // Note: lossy conversion
                    case XmlType::Integer:
                        return XmlDateTime::FromInteger(fromValue.ival);
                    case XmlType::String:
                        return XmlDateTime::FromString(fromValue.sval);
                    case XmlType::DateTime:
                        return fromValue.dtval;
                    case XmlType::Boolean: // fall-thru
                    default: {
                        XmlDateTime dt = { 0 };
                        return dt;
                    }
                }
                break;

            default:
                return XmlValue(); // this completely erases the input value
        }
    }
};

inline XmlValue FormatTimestamp(const XmlValue& ts, bool inMilliseconds = false)
{
    std::string input = ts.ToString();

    double rep;
    unsigned long long sec;
    int ms = 0;
    sscanf(input.c_str(), "%lg", &rep);
    const char* decPos = strchr(input.c_str(), '.');
    const char* fractionalms = "";
    if (inMilliseconds) {
        // input is in milliseconds with possible fraction of milliseconds
        if (decPos != nullptr) {
            fractionalms = decPos + 1;
        }
        unsigned long long val = std::strtoull(input.c_str(), nullptr, 0);
        sec = (val / 1000);
        ms = (int)(val % 1000);
    }
    else {
        // input is seconds with possible milliseconds as fraction
        if (decPos != nullptr) {
            ms = atoi(decPos + 1);
        }
        sec = std::strtoull(input.c_str(), nullptr, 0);
    }

    char s[64];
    time_t t = (time_t)sec;
    tm* ptm = localtime(&t);
    if (!ptm) {
        strcpy(s, "invalid");
    }
    else {
        sprintf(s, "%04d-%02d-%02d %02d:%02d:%02d.%03d%s", ptm->tm_year + 1900, ptm->tm_mon + 1, ptm->tm_mday,
            ptm->tm_hour, ptm->tm_min, ptm->tm_sec, ms, fractionalms);
    }
    return XmlValue(std::move(std::string(s)));
}

class XmlPath;
typedef std::shared_ptr<XmlPath> XmlPathPtr;

struct XmlPathRef
{
    enum Flags
    {
        Matched = 0x1,
        NoData = 0x2,
        AppendData = 0x4,
        Sync = 0x8,
        Joined = 0x10
    };

    XmlPathRef(const std::string& pathSpec, unsigned int flags = 0)
        : pathSpec(pathSpec)
        , flags(flags)
        , joinTableColIdx(-1)
    {
        parsedValue.type = XmlType::String;
    }

    operator std::string() const
    {
        return std::string("PathRef(") + pathSpec + ")";
    }

    XmlPathPtr path;
    std::string pathSpec;
    unsigned int flags;
    XmlValue parsedValue; // written by XmlPath
    std::vector<XmlExprPtr> startMatchExprs;
    std::vector<XmlExprPtr> endMatchExprs;
    size_t joinTableColIdx;
};

typedef std::shared_ptr<XmlPathRef> XmlPathRefPtr;

class XmlExpr
{
public:
    enum Flags
    {
        // Settings: not reset on each parse
        Visited = 0x1, // used in when validating structure
        SubtreeContainsAggregate = 0x2,
        SubtreeContainsInputPathRef = 0x4,
        SubtreeContainsJoinPathRef = 0x8,
        SubtreeContainsPathRef = SubtreeContainsInputPathRef | SubtreeContainsJoinPathRef,
        JoinEqualityWhere = 0x10
    };

    XmlExpr()
        : flags(0)
    {
    }

    unsigned int flags;

    void Clear() 
    {
        flags = 0;
        m_operator.reset();
        m_args.clear();
        m_pathRef.reset();
        m_columnRef.reset();
    }

    XmlValue& GetValue()
    {
        XmlValue& thisValue = GetValueRef();
        return thisValue;
    }

    void SetValue(const XmlValue& value)
    {
        XmlValue& thisValue = GetValueRef();
        thisValue = std::move(XmlValue::Convert(value, thisValue.type));
    }

    void SetValue(XmlValue&& value)
    {
        XmlValue& thisValue = GetValueRef();
        if (thisValue.type == value.type) {
            thisValue = std::move(value);
        }
        else {
            thisValue = std::move(XmlValue::Convert(value, thisValue.type));
        }
    }

    void SetValueAndType(const XmlValue& value)
    {
        XmlValue& thisValue = GetValueRef();
        thisValue = value;
    }

    XmlType GetType() const
    {
        const XmlValue& thisValue = GetValueRef();
        return thisValue.type;
    }

    void SetType(XmlType type)
    {
        XmlValue& thisValue = GetValueRef();
        thisValue.type = type;
    }

    void ChangeType(XmlType type)
    {
        XmlValue& thisValue = GetValueRef();
        thisValue = std::move(XmlValue::Convert(thisValue, type));
    }

    void SetOperator(XmlOperatorPtr op)
    {
        m_operator = op;
    }

    XmlOperatorPtr GetOperator() const
    {
        return m_operator;
    }

    size_t GetNumArgs() const
    {
        return m_args.size();
    }

    void AddArg(XmlExprPtr expr)
    {
        m_args.push_back(expr);
    }

    void SetArg(size_t argIdx, XmlExprPtr expr)
    {
        m_args[argIdx] = expr;
    }

    XmlExprPtr GetArg(size_t argNum) const
    {
        return m_args[argNum];
    }

    XmlPathRefPtr GetPathRef() const
    {
        return m_pathRef;
    }

    void SetPathRef(XmlPathRefPtr spec)
    {
        m_pathRef = spec;
    }

    XmlColumnPtr GetColumnRef() const
    {
        return m_columnRef;
    }

    void SetColumnRef(XmlColumnPtr column)
    {
        m_columnRef = column;
    }

private:
    XmlValue& GetValueRef()
    {
        return m_value;
    }

    const XmlValue& GetValueRef() const
    {
        return m_value;
    }

    XmlOperatorPtr m_operator;
    std::vector<XmlExprPtr> m_args;
    XmlValue m_value; // usually only one value
    XmlPathRefPtr m_pathRef; // used when opcode is OpPathRef
    XmlColumnPtr m_columnRef; // used when opcode is OpColumnRef
};

typedef std::shared_ptr<XmlExpr> XmlExprPtr;

struct XmlColumn
{
    enum Flags
    {
        Output = 0x1,
        Filter = 0x2,
        Aggregate = 0x4,
        JoinedColumn = 0x8,
        Indexed = 0x10, // set when a joined column is also where[] equality operand
        PivotResult = 0x20
    };

    XmlColumn(const std::string& name, XmlExprPtr expr = nullptr, unsigned int flags = 0)
        : name(name)
        , expr(expr)
        , flags(flags)
        , index(npos) // set by XmlQuerySpec
        , valueIdx(npos) // set by XmlQuerySpec
    {
    }

    std::string name;
    XmlExprPtr expr;
    unsigned int flags;
    size_t index;
    size_t valueIdx;

    operator std::string()
    {
        return std::string("Column(") + name + ")";
    }

    bool IsOutput() const
    {
        return !!(flags & Output);
    }

    bool IsFilter() const
    {
        return !!(flags & Filter);
    }

    bool IsAggregate() const
    {
        return !!(flags & Aggregate);
    }

    bool IsPivotResult() const
    {
        return !!(flags & PivotResult);
    }
};

typedef std::shared_ptr<XmlColumn> XmlColumnPtr;
typedef std::vector<XmlColumnPtr> XmlColumns;
typedef std::vector<XmlExprPtr> XmlExprs;

struct IColumnEditor
{
    virtual const XmlColumns& GetColumns() const = 0;
    virtual XmlColumnPtr GetColumn(const std::string& colName) const = 0;
    virtual size_t InsertColumn(XmlColumnPtr column, size_t idx = npos) = 0;
    virtual void DeleteColumn(XmlColumnPtr column) = 0;
};

struct XmlNodeInfo
{
    XmlNodeInfo(const std::string& name, size_t nodeStart)
        : name(name)
        , nodeStart(nodeStart)
    {
    }

    XmlNodeInfo(const char* name, size_t len, size_t nodeStart)
        : name(name, len)
        , nodeStart(nodeStart)
    {
    }

    std::string name;
    size_t nodeStart;
};

typedef std::vector<XmlValue> XmlRow; // one for each non-aggregate column value
typedef std::vector<XmlRow> XmlRows;
typedef std::shared_ptr<std::vector<XmlRow>> XmlRowsPtr;
typedef std::unordered_map<size_t, XmlRowsPtr> XmlIndexedRows;

inline size_t hashXmlRow(const XmlRow& row, const std::vector<size_t>& indices)
{
    static auto intHasher = std::hash<__int64_t>();
    static auto boolHasher = std::hash<bool>();
    static auto doubleHasher = std::hash<double>();
    static auto stringHasher = std::hash<std::string>();
    size_t seed = 0;
    for (size_t i = 0; i < indices.size(); i++) {
        const XmlValue& v = row[indices[i]];
        size_t a;
        switch (v.type) {
            case XmlType::Real:
                a = doubleHasher(v.rval);
                break;
            case XmlType::Integer:
                a = intHasher(v.ival);
                break;
            case XmlType::Boolean:
                a = boolHasher(v.bval);
                break;
            case XmlType::DateTime:
                a = doubleHasher(*(double*)(&v.dtval));
                break;
            case XmlType::String:
                a = stringHasher(v.sval);
                break;
            default:
                a = 0;
                break;
        }
        seed ^= a + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
}

inline bool equalsXmlRow(const XmlRow& left, const XmlRow& right, size_t length)
{
    for (size_t i = 0; i < length; i++) {
        if (XmlValue::Compare(left[i], right[i]) != 0) {
            return false;
        }
    }
    return true;
}

// custom hash functor, used to exclude sort values, which are at the end of the row vector
class XmlRowHash
{
public:
    XmlRowHash(size_t seqLength)
    {
        for (size_t i = 0; i < seqLength; i++) {
            indices.push_back(i);
        }
    }
    
    XmlRowHash(const std::vector<size_t>& indices)
    {
        this->indices = indices;
    }

    size_t operator()(const XmlRow& row) const
    {
        return hashXmlRow(row, indices);
    }

private:
    std::vector<size_t> indices;
};

// custom equals functor
class XmlRowEquals
{
public:
    XmlRowEquals(size_t length)
    {
        this->length = length;
    }

    size_t operator()(const XmlRow& left, const XmlRow& right) const
    {
        return equalsXmlRow(left, right, length);
    }

private:
    size_t length;
};

enum XmlPassType
{
    PassNotSet,
    GatherDataPass, // reads input, do precompute evaluations (e.g. kmeans extension), no output
    MainPass, // reads input (again), main evaluations, build non-aggregate domain, build aggregates, output if
              // StoredValuesPass not needed
    StoredValuesPass // output values saved for the purpose of sorting, aggregation, and distinct
};

struct XmlParserContext
{
    XmlParserContext()
    {
        Reset(XmlPassType::PassNotSet);
    }

    void Reset(XmlPassType _passType)
    {
        passType = _passType;
        appendingValues = false;
        numNodes = 0;
        numLines = 1;
        numRowsOutput = 0;
        numRowsMatched = 0;
        relativeDepth = 0;
        nodeStack.clear();
        attrCountStack.clear();
        attrStack.clear();
        ResetJoinTable();
    }

    void SetJoinTable(XmlRowsPtr _joinTable) 
    {
        joinTable = _joinTable;
        emptyOuterJoin = false;
        joinTableRowIdx = 0;
    }

    void ResetJoinTable() 
    {
        joinTable.reset();
        emptyOuterJoin = false;
        joinTableRowIdx = -1;
    }

    // Context controlled through XmlParser::Reset
    XmlPassType passType;
    bool appendingValues;
    size_t numNodes; 
    size_t numLines;
    size_t numRowsMatched; // before filtering
    size_t numRowsOutput; // after filtering
    int relativeDepth;
    std::vector<XmlNodeInfo> nodeStack;
    std::vector<int> attrCountStack;
    std::vector<std::pair<std::string, std::string>> attrStack;
    
    // Join table data
    XmlRowsPtr joinTable;
    size_t joinTableRowIdx;
    bool emptyOuterJoin;
};

typedef std::shared_ptr<XmlParserContext> XmlParserContextPtr;

} // namespace StreamingXml

// Add more template specializations for _print (see end of xmlutils.h)
template <> void _print(StreamingXml::XmlType v)
{
    std::cout << GetName(v); // assuming T implements operator std::string
}

template <> void _print(StreamingXml::XmlRow v)
{
    std::cout << "[";
    for (size_t i = 0; i < v.size(); i++) {
        if (i > 0) {
            std::cout << ", ";
        }
        _print(v[i]);
    }
    std::cout << "]";
}

namespace std
{
using namespace StreamingXml;

// default hash functor
template <> struct hash<XmlRow>
{
    size_t operator()(const XmlRow& row) const
    {
        StreamingXml::XmlRowHash hash(row.size());
        return hash(row);
    }
};

} // namespace std

