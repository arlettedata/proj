/* *      Streaming XML Parser.
 *
 *      Copyright (c) 2005-2017 by Brian. Kramer
 *
 * This software is provided 'as-is', without any express or implied
 *
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
#include <cstdio>
#include <float.h>
#include <fstream>
#include <limits.h>
#include <string.h>
#include <string>
#include <sstream>
#include <vector>
#include <math.h>

namespace StreamingXml
{

class XmlUtils
{
public:
    static bool IsWin32()
    {
#ifdef _WIN32
        return true;
#else
        return false;
#endif
    }

    static void die(const std::string& msg, const std::string& token1 = "", const std::string& token2 = "",
        const std::string& token3 = "", const std::string& token4 = "")
    {
        std::string s;
        s.resize(msg.length() + token1.length() + token2.length() + token3.length() + token4.length() + 1);
        sprintf(&s[0], msg.c_str(), token1.c_str(), token2.c_str(), token3.c_str(), token4.c_str());
        throw std::runtime_error(s.c_str());
    }

    static char* ToLower(char* str)
    {
        for (char* pos = str; *pos != '\0'; pos++) {
            *pos = (char)tolower(*pos);
        }
        return str;
    }

    static std::string& ToLower(std::string& str)
    {
        ToLower((char*)str.c_str());
        return str;
    }

    static char* ToUpper(char* str)
    {
        for (char* pos = str; *pos != '\0'; pos++) {
            *pos = (char)toupper(*pos);
        }
        return str;
    }

    static std::string& ToUpper(std::string& str)
    {
        ToUpper((char*)str.c_str());
        return str;
    }

    static void Replace(std::string& str, const std::string& from, const std::string& to) 
    {
        for (size_t pos = 0; (pos = str.find(from, pos)) != std::string::npos; pos += to.size()) {
            str.replace(pos, from.size(), to);
        }
    }

    template <class T> static std::string ToString(T t)
    {
        std::stringstream ss;
        ss.setf(std::ios::boolalpha);
        ss << t;
        return move(ss.str());
    }

    static std::string ToString(double d, int precision)
    {
        char buf[1024];
        int pos = std::snprintf(buf, sizeof(buf), "%.*f", precision, d) - 1;
        // Trim all trailing zeros, except one immediately after the decimal point
        while (pos >= 2 && buf[pos] == '0' && buf[pos - 1] != '.') {
            buf[pos--] = '\0';
        }
        return move(std::string(buf));
    }

    static std::vector<std::string>& Split(const std::string& input, std::vector<std::string>& output,
        const std::string& delimiters = "", const std::string& quoters = "\"", bool insertGaps = false,
        std::vector<const char*>* positions = nullptr)
    {
        output.clear();

        const char* delims = delimiters.c_str();
        bool gap = true; // when we hit first delimiter, it's a gap
        for (const char* pos = input.c_str(); *pos != '\0';) {
            while (strchr(delims, *pos) && (*pos != '\0')) {
                if (insertGaps && gap) {
                    output.push_back("");
                }
                gap = true;
                pos++;
            }
            if (*pos == '\0') {
                break;
            }
            const char* beg = pos;
            bool inQuotes = false;
            bool escaping = false;
            while ((inQuotes || escaping || (strchr(delims, *pos) == 0)) && (*pos != '\0')) {
                if (escaping) {
                    escaping = false;
                } 
                else if (*pos == '\\') {
                    escaping = true;
                }
                else if (quoters.find(*pos) != std::string::npos) {
                    inQuotes = !inQuotes;
                }
                pos++;
            }

            size_t len = pos - beg;
            if (positions) {
                positions->push_back(beg);
            }
            output.push_back(std::string(beg, len));
            gap = false; // next delimiter is not a gap
        }

        return output;
    }

    static std::vector<std::string> Split(const std::string& input, const std::string& delimiters = " ",
        const std::string& quoters = "\"", bool insertGaps = false)
    {
        std::vector<std::string> arr;
        Split(input, arr, delimiters, quoters, insertGaps);
        return std::move(arr);
    }

    static std::string UnescapeCharacters(const std::string& str)
    {   
        #define CH(offset, ch) (str[pos + offset] == ch)
        std::string result;
        result.reserve(str.size() * 2);
        size_t inputLen = str.length();
        size_t matchLen = 0;
        size_t remaining = inputLen;
        char buf[4];
        for (size_t pos = 0; pos < inputLen; pos += matchLen, remaining -= matchLen) {
            matchLen = 0;
            char ch = str[pos];
            if (ch == '\\' && remaining >= 2) {
                char ch2 = str[pos + 1];
                if (ch2 == '\\') {
                    result.append("\\"), matchLen = 2;
                }
                else if (ch2 == '\"') {
                    result.append("\""), matchLen = 2;
                }
                else if (ch2 == 'n') {
                    result.append("\n"), matchLen = 2;
                }
                else if (ch2 == 'r') {
                    result.append("\r"), matchLen = 2;
                }
                else if (ch2 == 't') {
                    result.append("\t"), matchLen = 2;
                }
            }
            else if (ch == '&') {
                if (remaining >= 4 && CH(1, 'l') && CH(2, 't') && CH(3, ';')) {
                    result.append("<"), matchLen = 4; // matched &lt;
                }
                else if (remaining >= 4 && CH(1, 'g') && CH(2, 't') && CH(3, ';')) {
                    result.append(">"), matchLen = 4; // matched &gt;
                }
                else if (remaining >= 5 && CH(1, 'a') && CH(2, 'm') && CH(3, 'p') && CH(4, ';')) {
                    result.append("&"), matchLen = 5; // matched &amp;
                }
                else if (remaining >= 6 && CH(1, 'q') && CH(2, 'u') && CH(3, 'o') && CH(4, 't') && CH(5, ';')) {
                    result.append("\""), matchLen = 6; // matched &quot;
                }
                else if (remaining >= 6 && CH(1, 'a') && CH(2, 'p') && CH(3, 'o') && CH(4, 's') && CH(5, ';')) {
                    result.append("\'"), matchLen = 6; // matched &apos;
                }
                else if (remaining >= 5 && CH(1, '#') && CH(2, 'x')) {
                    // matching &#xH; and &#xHH;
                    char c = str[pos + 3];
                    if (c >= '0' && c <= '9') {
                        ch = c - '0';
                    }
                    else if (c >= 'A' && c <= 'F') {
                        ch = c - 'A' + 10;
                    }
                    else if (c >= 'a' && c <= 'f') {
                        ch = c - 'a' + 10;
                    }
                    matchLen = 5;
                    if (remaining >= 6 && str[pos + 4] != ';') {
                        ch *= 16;
                        c = str[pos + 4];
                        if (c >= '0' && c <= '9') {
                            ch += c - '0';
                        }
                        else if (c >= 'A' && c <= 'F') {
                            ch += c - 'A' + 10;
                        }
                        else if (c >= 'a' && c <= 'f') {
                            ch += c - 'a' + 10;
                        }
                        matchLen++;
                    }
                    buf[0] = ch;
                    buf[1] = 0;
                    result.append(buf);
                }
                else if (remaining >= 4 && CH(1, '#')) {
                    // matching &#D; and &#DD;
                    char c = str[pos + 2];
                    ch = c - '0';
                    if (remaining >= 5 && str[pos + 3] != ';') {
                        ch *= 10;
                        c = str[pos + 3];
                        if (c >= '0' && c <= '9') {
                            ch += c - '0';
                        }
                    }
                    buf[0] = ch;
                    buf[1] = 0;
                    result.append(buf);
                    matchLen = 5;
                }
            }
            if (!matchLen) {
                matchLen = 1;
                buf[0] = ch;
                buf[1] = 0;
                result.append(buf);
            }
        }
        #undef CH
        return move(result);
    }

    static std::string& Unquote(std::string& str)
    {
        size_t len = str.length();
        if (len > 0) {
            for (size_t pos = 0; pos < len - 1; pos++) {
                char ch = str[pos];
                if (ch == '\"') {
                    str.erase(pos, 1); // remove quote
                }
                else if (ch == '\\') {
                    pos++; // skip following character
                }
            }
        }
        return str;
    }

    static std::vector<std::string>& Unquote(std::vector<std::string>& strs)
    {
        for (size_t i = 0; i < strs.size(); i++) {
            Unquote(strs[i]);
        }
        return strs;
    }

    static std::string& TrimTrailingWhitespace(std::string& str)
    {
        const char* start = str.c_str();
        const char* end = start + strlen(start) - 1;

        while (start <= end) {
            if (isspace(*end) || (*end == '\r') || (*end == '\n')) {
                end--;
            }
            else {
                break;
            }
        }

        str.erase((size_t)end - (size_t)start + 1);
        return str;
    }

    static char* TrimTrailingWhitespace(char* str)
    {
        char* start = str;
        char* end = start + strlen(start) - 1;
        while (start <= end) {
            if (isspace(*end) || (*end == '\r') || (*end == '\n')) {
                end--;
            }
            else {
                break;
            }
        }

        *(++end) = '\0';
        return str;
    }

    static std::string& TrimWhitespace(std::string& str)
    {
        int eraseFront = 0;
        char* beg = (char*)str.c_str();
        while ((*beg == ' ') || (*beg == '\t') || (*beg == '\r') || (*beg == '\n')) {
            beg++, eraseFront++;
        }
        if (eraseFront > 0) {
            str.erase(0, eraseFront);
        }

        beg = (char*)str.c_str();
        char* end = (char*)str.c_str() + str.length() - 1;
        while ((end >= beg) && ((*end == ' ') || (*end == '\t') || (*beg == '\r') || (*end == '\n'))) {
            end--;
        }
        str.erase(end - beg + 1);
        return str;
    }

    static char* TrimWhitespace(char* str)
    {
        char* beg = str;
        while (isspace(*beg)) {
            beg++;
        }
        if (str != beg) {
            memmove(str, beg, strlen(beg) + 1);
        }
        return TrimTrailingWhitespace(str);
    }

    static std::string& AppendSlash(std::string& path, bool local = false)
    {
        if ((path.back() != '\\') && (path.back() != '/')) {
            path.append((IsWin32() && local) ? "\\" : "/");
        }
        return path;
    }

    static std::string& AppendPath(std::string& path, const std::string& subpath, bool 
        appendSlash = true, bool local = false)
    {
        path += subpath;
        if (appendSlash) {
            AppendSlash(path, local);
        }
        return path;
    }

    // XmlUtils::GetLine doesn't work with all line endings, MacOS \r in particular. Use this instead.
    // Returns true if eof has NOT been yet encountered (i.e. keep reading)
    // Adapteds from https://stackoverflow.com/questions/6089231/getting-std-ifstream-to-handle-lf-cr-and-crlf.
    static bool GetLine(std::istream& is, std::string& line)
    {
        line.clear();

        auto print = [](std::string s) { std::cout << s << std::endl; };

        // The characters in the stream are read one-by-one using a std::streambuf.
        // That is faster than reading them one-by-one using the std::istream.
        // Code that uses streambuf this way must be guarded by a sentry object.
        // The sentry object performs various tasks,
        // such as thread synchronization and updating the stream state.

        std::istream::sentry se(is, true);
        std::streambuf* sb = is.rdbuf();

        while (true) {
            int c = sb->sbumpc();
            switch (c) {
                case 10:
                    return true;
                case 13:
                    if(sb->sgetc() == 10) {
                        sb->sbumpc();
                    } else {
                    }
                    return true;
                case std::streambuf::traits_type::eof():
                    // Also handle the case when the last line has no line ending
                    if (line.empty()) {
                        is.setstate(std::ios::eofbit);
                        return false;
                    } 
                    return true;
                default:
                    line += (char)c;
            }
        }
    }

    static bool ParseBoolean(const char* s, bool* exactMatch = nullptr)
    {
        assert(s);

        // exact matches
        if (exactMatch) {
            *exactMatch = true;
        }
        if (strcasecmp(s, "false") == 0 || strcasecmp(s, "0") == 0) {
            return false;
        }
        if (strcasecmp(s, "true") == 0 || strcasecmp(s, "1") == 0) {
            return true;
        }

        // inexact matches
        if (exactMatch) {
            *exactMatch = false;
        }

        switch (tolower(s[0])) {
            case '1':
            case 'y':
            case 't':
                return true;
            default:
                return false;
        }
    }

    static __int64_t ParseInteger(const char* s, bool* exactMatch = nullptr)
    {
        assert(s);
        char* e;
        __int64_t i = strtoll(s, &e, 10);
        if (exactMatch) {
            *exactMatch = (*e == '\0') || (*e == ' ');
        }
        return i;
    }

    static double ParseReal(const char* s, bool* exactMatch = 0)
    {
        assert(s);
        char* e;
        double d = strtod(s, &e);
        if (exactMatch) {
            *exactMatch = (*e == '\0') || (*e == ' ');
        }
        return d;
    }

    static double nan()
    {
        return sqrt(-1);
    }

    static bool CaseSensitivityMode(bool set = false, bool newValue = false) 
    {
        static bool value = false;
        if (set) {
            value = newValue;
        }
        return value;
    }

    static bool stringsEqCase(const std::string& a, const std::string& b)
    {
        if (CaseSensitivityMode()) {
            return strcmp(a.c_str(), b.c_str()) == 0;
        }
        return strcasecmp(a.c_str(), b.c_str()) == 0;
    }

    static bool stringsEqCase(const std::string& a, const std::string& b, size_t len)
    {
        if (CaseSensitivityMode()) {
            return strncmp(a.c_str(), b.c_str(), len) == 0;
        }
        return strncasecmp(a.c_str(), b.c_str(), len) == 0;
    }

    static bool stringsEqCase(const char* a, const char* b, size_t len)
    {
        if (CaseSensitivityMode()) {
            return strncmp(a, b, len) == 0;
        }
        return strncasecmp(a, b, len) == 0;
    }

    static std::string CsvNormalize(const std::string& s)
    {
        bool needsQuotes = false;
        char sep = ',';
        for (size_t i = 0; i < s.size() && !needsQuotes; i++) {
            needsQuotes = s[i] == sep || s[i] == '\"' || s[i] == '\n';
        }
        if (!needsQuotes) {
            return s;
        }
        std::stringstream ss;
        ss << '\"';
        for (size_t i = 0; i < s.size(); i++) {
            if (s[i] == '\"') {
                ss << '\"';
            }
            ss << s[i];
        }
        ss << '\"';
        return std::move(ss.str().c_str());
    }

    static std::string FormatForCsv(const std::string& value)
    {
        return std::move(CsvNormalize(std::move(XmlUtils::UnescapeCharacters(value))));
    }

    static void Error(const std::string& msg, const std::string& token1 = "")
    {
        std::string s;
        s.resize(msg.size() + token1.size() + 1);
        sprintf(&s[0], msg.c_str(), token1.c_str());
        XmlUtils::die(s);
    }

    static void Error(const std::string& msg, const std::string& token1, const std::string& token2)
    {
        std::string s;
        s.resize(msg.size() + token1.size() + token2.size() + 1);
        sprintf(&s[0], msg.c_str(), token1.c_str(), token2.c_str());
        XmlUtils::die(s);
    }

    static void Error(
        const std::string& msg, const std::string& token1, const std::string& token2, const std::string& token3)
    {
        std::string s;
        s.resize(msg.size() + token1.size() + token2.size() + token3.size() + 1);
        sprintf(&s[0], msg.c_str(), token1.c_str(), token2.c_str(), token3.c_str());
        XmlUtils::die(s);
    }
}; // class XmlUtils

#ifdef _WIN32
extern "C" int __stdcall SetConsoleCtrlHandler(bool(__stdcall*)(unsigned int), bool);
#endif

class ControlCHandler
{
public:
    ControlCHandler()
    {
        Install(true);
    }

    ~ControlCHandler()
    {
        Install(false);
    }

    static bool ControlCIssued(bool setState = false)
    {
        static bool state = false;
        if (setState) {
            state = true;
        }
        return state;
    }

private:
#ifdef _WIN32
    void Install(bool install)
    {
        static int installCount = 0;
        installCount += install ? 1 : -1;
        if (installCount == (install ? 1 : 0)) {
            SetConsoleCtrlHandler(CtrlHandler, install);
        }
    }

    static bool __stdcall CtrlHandler(unsigned int ctrltype)
    {
        if (ctrltype == 0) { // 0 is CTRL_C_EVENT
            ControlCIssued(true);
            return true;
        }
        return false;
    }
#else
    void Install(bool /*increment*/)
    {
        // control-C handled differently under UNIX
    }
#endif
};

static bool ControlCIssued()
{
    return ControlCHandler::ControlCIssued();
}

} // namespace StreamingXml

// convenience debug printer: print("foo",1) => "foo 1"

template <typename T> void _print(const T v)
{
    std::cout << (std::string)v; // assuming T implements operator std::string
}
template <> void _print(bool v)
{
    std::cout << (v ? "true" : "false");
}
template <> void _print(int v)
{
    std::cout << v;
}
template <> void _print(double v)
{
    std::cout << v;
}
template <> void _print(__int64_t v)
{
    std::cout << v;
}
template <> void _print(size_t v)
{
    std::cout << v;
}
template <typename T> void _print(const std::shared_ptr<T> v)
{
    _print(*v);
}
void print()
{
    std::cout << std::endl;
}
template <typename T, typename... Args> void print(const T v, Args... args)
{
    _print(v);
    std::cout << " ";
    print(args...);
}
