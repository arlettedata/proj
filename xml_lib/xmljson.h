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

#include <iostream>
#include <exception>
#include <queue>

#include "xmlutils.h"
#include "xmloutput.h"

namespace StreamingXml
{

class XmlTag;
class XmlOutput;

// If the Json Parser reads an initial less-than character, which is the start of Xml,
// we will want to switch to the Xml parser.
// This exception both signals that condition, and provides the buffer for reparsing.
class XmlInputException : public std::exception
{
public:
    XmlInputException(const std::string& possibleFormat)
        : m_possibleFormat(possibleFormat)
    {
    }

    const std::string& getPossibleFormat() const
    {
        return m_possibleFormat;
    }

private:
    std::string m_possibleFormat;
};

enum JsonToken {
	TOKEN_NONE, TOKEN_OPENBRACE, TOKEN_CLOSEBRACE, TOKEN_OPENBRACKET, TOKEN_CLOSEBRACKET,
	TOKEN_LESSTHAN, TOKEN_COMMA, TOKEN_COLON, TOKEN_STRING, TOKEN_VALUE, TOKEN_LINEFEED, TOKEN_EOF, TOKEN_INVALID
};

enum States { STATE_TOP, STATE_START, STATE_READCOLON, STATE_READVALUE, STATE_ARRAY };

class illegal_char_exception : public std::exception
{
public:
	illegal_char_exception(const char ch)
	{
		m_value += ch;
	}

	virtual const char* what() const throw()
	{
		return m_value.c_str();
	}

private:
	std::string m_value;
};

class unexpected_eof_exception : public std::exception
{
public:
	unexpected_eof_exception()
	{
	}

	virtual const char* what() const throw()
	{
		return "Unexpected EOF";
	}

private:
	std::string m_value;
};

class unexpected_token_exception : public std::runtime_error
{
public:
	unexpected_token_exception(const std::string& str)
		: std::runtime_error(str)
	{
	}

private:
	std::string m_value;
};

static std::string emptyStr;

class Json2Xml
{
public:
    Json2Xml(std::shared_ptr<XmlOutput> xml, const std::string& topTag, bool leaveOuterTagOpen = false)
        : m_in(nullptr)
		, m_xml(xml)
		, m_topTag(MakeTag(topTag))
		, m_leaveOuterTagOpen(leaveOuterTagOpen)
		, m_lastChar('\0')
		, m_currChar('\0')
		, m_charCount(0)
		, m_lineCount(1)
		, m_possiblyXml(true)
		, m_possiblyLog(true)
	{
	}

    const std::string& GetBackBuffer() const
    {
        return m_backBuffer;
    }
        
    std::shared_ptr<XmlTag> Read(std::istream& in)
    {
        std::shared_ptr<XmlTag> emptyTag;
        m_in = &in;
        if (m_in->eof()) {
            return std::move(emptyTag);
        }

        std::string value;
        JsonToken token = TOKEN_NONE;
        while (token != TOKEN_EOF) {
            token = GetNextToken(value);
            switch (token) {
                case TOKEN_OPENBRACE:
                    m_possiblyXml = false;
                    m_possiblyLog = false;
                    return ParseObject(m_topTag, 0, m_leaveOuterTagOpen);

                case TOKEN_OPENBRACKET:
                    if (!m_possiblyLog)
                        goto def;
                    do {
                        token = GetNextToken(value);
                    } while (token == TOKEN_LINEFEED);
                    #pragma warning(disable:4996)
                    int year, month, day;
                    if (3 == sscanf(value.c_str(), "%d-%d-%d", &year, &month, &day)) {
                        // treat input as a log4j-style log because we've seen something like [2015-10-10 ...
                        throw XmlInputException("log");
                    }
                    else {
                        // treat this as json expressing a top-level (unnamed) array
                        UngetToken(token, value);
                        m_possiblyXml = false;
                        m_possiblyLog = false;
                        std::shared_ptr<XmlTag> tag( new XmlTag(*m_xml, m_topTag.size() ? m_topTag : "arr"));
                        ParseArray("row");
                        return tag;
                    }
                    break;

                case TOKEN_LESSTHAN:
                    if (!m_possiblyXml) {
                        goto def;
                    }
                    // treat input as xml
                    throw XmlInputException("xml");
                    break;

                case TOKEN_EOF:
                case TOKEN_LINEFEED:
                    break;

                default:
    def:
                    UnexpectedToken(STATE_TOP, __FUNCTION__, token, value);
            }
        }
        return std::move(emptyTag);
    }
	
private:
	char getch()
	{
		m_lastChar = m_currChar;
		m_currChar = m_in->get();
		if( m_possiblyXml || m_possiblyLog ) {
			m_backBuffer += m_currChar;
		}
		m_charCount++;
		if (m_currChar == '\n') {
			m_lineCount++;
		}
		if (m_currChar == '\r') {
			return getch(); // ignore the '\r'
		}
		if ((m_currChar >= 0x0 && m_currChar < 0x10) && (m_currChar != '\n')) {
			throw illegal_char_exception(m_currChar); // json, thankfully, disallows control characters, so this lets us detect truncated input
		}
#ifdef _DEBUG
		m_recallBuffer += m_currChar;
		if (m_recallBuffer.size() > 256) {
			m_recallBuffer = m_recallBuffer.substr(128);
		}
#endif
		return m_currChar;
	}

	void ungetch()
	{
		m_in->unget();
		if( (m_possiblyXml || m_possiblyLog) && m_backBuffer.length() > 0 ) {
			m_backBuffer.pop_back();
		}
#ifdef _DEBUG
		if (m_recallBuffer.size())
			m_recallBuffer.erase(m_recallBuffer.size() - 1);
#endif
	}

    std::string ReadString(char quoteChar)
    {
        // assumes we've already read the open quote
        std::string str;
        int ch = 0;
        bool done = false, escaped = false;
        while (!done) {
            ch = getch();
            switch (ch) {
                case -1:
                    throw unexpected_eof_exception();
                case '\"':
                case '\'':
                    if (ch == quoteChar && !escaped) {
                        done = true;
                    }
                    else {
                        str += ch;
                    }
                    break;
                case 'r':
                    str += escaped ? '\r' : ch;
                    break;
                case 'n':
                    str += escaped ? '\n' : ch;
                    break;
                case 't':
                    str += escaped ? '\t' : ch;
                    break;
                case '\\':
                    if (escaped) {
                        str += '\\';
                        escaped = false;
                    } 
                    else {
                        escaped = true;
                    }
                    break;
                default:
                    if (escaped) {
                        str += '\\';
                    }
                    str += ch;
                    break;
            }
            if (ch != '\\') {
                escaped = false;
            }
        }
        return std::move(str);
    }

    std::string ReadValue(char initialChar)
    {
        std::string str;
        str += initialChar;
        bool done = false;
        while (!done) {
            auto ch = getch();
            switch (ch) {
                case '\r':
                case '\n':
                case ' ':
                case ',':
                case '}':
                case ']':
                case ':':
                    ungetch();
                    done = true;
                    break;
                case -1:
                    done = true;
                    break;
                case '\\':
                    continue;
                default:
                    str += ch;
            }
        }
        return std::move(str);
    }

    void UnexpectedToken(States state, const std::string& function, JsonToken token, const std::string& value)
    {
        std::stringstream msg;
        msg << "Unexpected token: char=" << m_charCount << ", line=" << m_lineCount << ", function=" << function << ", token=";
        switch (token) {
            case TOKEN_OPENBRACE:
                msg << "{";
                break;
            case TOKEN_CLOSEBRACE:
                msg << "}";
                break;
            case TOKEN_OPENBRACKET:
                msg << "[";
                break;
            case TOKEN_CLOSEBRACKET:
                msg << "]";
                break;
            case TOKEN_LESSTHAN:
                msg << "<";
                break;
            case TOKEN_COMMA:
                msg << ",";
                break;
            case TOKEN_COLON:
                msg << ":";
                break;
            case TOKEN_STRING:
                msg << "String(" << value << ")";
                break;
            case TOKEN_VALUE:
                msg << "Value(" << value << ")";
                break;
            case TOKEN_LINEFEED:
                msg << "<LF>";
                break;
            case TOKEN_EOF:
                msg << "<EOF>";
                break;
            default:
                msg << "<unknown token>";
        }

        msg << ", state=";
        switch (state) {
            case STATE_TOP:
                msg << "Top";
                break;
            case STATE_START:
                msg << "Start";
                break;
            case STATE_READCOLON:
                msg << "ReadColon";
                break;
            case STATE_READVALUE:
                msg << "ReadValue";
                break;
            case STATE_ARRAY:
                msg << "Array";
                break;
            default:
                msg << "<unknown state: " << state << ">";
        }
        msg << std::endl;
    #ifdef _DEBUG
        // feed more characters into the recall buffer
        for (int i = 0; i < 32; i++) {
            getch();
        }
        msg << "Input region: " << m_recallBuffer;
    #endif
        throw unexpected_token_exception(msg.str());
    }

    void UngetToken(JsonToken token, const std::string& value)
    {
        m_ungetTokens.push(std::pair<JsonToken, std::string>(token, value));
    }

    JsonToken GetNextToken( /*out*/ std::string& value)
    {
        if (m_ungetTokens.size()) {
            auto p = m_ungetTokens.front();
            m_ungetTokens.pop();
            value = p.second;
            return p.first;
        }

        while (true) {
            auto ch = getch();
            switch (ch) {
                case ' ':
                    continue;
                case '{':
                    return TOKEN_OPENBRACE;
                case '}':
                    return TOKEN_CLOSEBRACE;
                case '<':
                    return TOKEN_LESSTHAN;
                case ':':
                    return TOKEN_COLON;
                case ',':
                    return TOKEN_COMMA;
                case '[':
                    return TOKEN_OPENBRACKET;
                case ']':
                    return TOKEN_CLOSEBRACKET;
                case '\"':
                case '\'':
                    value = ReadString(ch);
                    return TOKEN_STRING;
                case -1:
                    return TOKEN_EOF;
                case '\n':
                    return TOKEN_LINEFEED;
                default:
                    if (ch >= 0 && ch < 32) {
                        continue;
                    }
                    value = ReadValue(ch);
                    return TOKEN_VALUE;
            }
        }
    }

    std::shared_ptr<XmlTag> ParseObject(const std::string& firstName, int depth = 0, bool dontCloseTag = false)
    {
        States state = STATE_START;
        std::shared_ptr<XmlTag> xmlTag;  // closes when it goes out of scope, except when dontCloseTag is true
        std::shared_ptr<XmlTag> outerTag;
        std::string name = firstName, value;
        bool getAttributes = false; // true when we just read "_attr" name

        for (JsonToken token = TOKEN_NONE; token != TOKEN_CLOSEBRACE;) {
            token = GetNextToken(/*out*/ value);
            switch (token) {
                case TOKEN_STRING:
                case TOKEN_VALUE: // includes identifiers
                    // Note that we are allow non-JSON-like case of {x:value}, where token is the identifier x, and state is STATE_START.
                    // (Real JSON strings as names: {"x":value})
                    if (state == STATE_START) {
                        if (name.size() && value == "_attr" && !getAttributes) {
                            getAttributes = true;
                        }
                        else {
                            if (name.size()) {
                                xmlTag.reset(new XmlTag(*m_xml, MakeTag(name)));
                            }
                            name = value;
                        }
                        state = STATE_READCOLON;
                    }
                    else if (state == STATE_READVALUE) {
                        getAttributes = false; // e.g. _attr: 1 instead of _attr: {...}
                        m_xml->AddData(MakeTag(name), value);
                        name = "";
                        state = STATE_START;
                    }
                    else {
                        UnexpectedToken(state, __FUNCTION__, token, value);
                    }
                    break;

                case TOKEN_COLON:
                    if (state != STATE_READCOLON) {
                        UnexpectedToken(state, __FUNCTION__, token, value);
                    }
                    state = STATE_READVALUE;
                    break;

                case TOKEN_OPENBRACE:
                    if (state != STATE_READVALUE) {
                        UnexpectedToken(state, __FUNCTION__, token, value);
                    }
                    if (getAttributes) {
                        std::string attr = ParseAttributes();
                        xmlTag.reset(new XmlTag(*m_xml, MakeTag(name), XmlOutput::All, attr));
                        getAttributes = false;
                    }
                    else {
                        // Outer tags are discovered on second descent, i.e. second open brace.  
                        // Ex: { foo: {...}, bar: {...} } makes foo the outer tag, and bar gets nested in it so we have a better definition
                        outerTag = ParseObject(name, depth + 1, dontCloseTag);
                    }
                    name = "";
                    state = STATE_START;
                    break;

                case TOKEN_OPENBRACKET:
                    if (state != STATE_READVALUE) {
                        UnexpectedToken(state, __FUNCTION__, token, value);
                    }
                    getAttributes = false; // e.g. _attr: [1] instead of _attr: {...}
                    ParseArray(name);
                    name = "";
                    state = STATE_START;
                    break;

                case TOKEN_COMMA:
                    if (state != STATE_START) {
                        UnexpectedToken(state, __FUNCTION__, token, value);
                    }
                    state = STATE_START; // commas are not informative in our implementation, and we don't check for misplaced commas
                    break;

                case TOKEN_CLOSEBRACE:
                    if (state != STATE_START) {
                        UnexpectedToken(state, __FUNCTION__, token, value);
                    }
                    if (name.size()) { // If no name/values were read, then emit the tag with no content
                        xmlTag.reset(new XmlTag(*m_xml, MakeTag(name)));
                    }
                    name = "";
                    break;

                case TOKEN_LINEFEED:
                    break;

                case TOKEN_EOF:
                    throw unexpected_eof_exception();

                default:
                    UnexpectedToken(state, __FUNCTION__, token, value);
            }

            // Capture the outer tag.  Outer tags happen at depth = 0 for {a:1} and depth = 1 for {a:{b:1}}
            if (depth <= 1 && xmlTag.get() ) {
                outerTag = xmlTag;
            }
        }

        if (!dontCloseTag) {
            xmlTag.reset(); 
        }

        return outerTag;
    }

    std::string ParseAttributes()
    {
        States state = STATE_START;
        std::stringstream accumAttributes;
        std::string name, value;

        for (JsonToken token = TOKEN_NONE; token != TOKEN_CLOSEBRACE;) {
            token = GetNextToken(/*out*/ value);
            switch (token) {
                case TOKEN_STRING:
                case TOKEN_VALUE:
                    if (state == STATE_START) {
                        name = value;
                        state = STATE_READCOLON;
                    }
                    else if (state == STATE_READVALUE) {
                        if (strchr(value.c_str(), '\"') != 0) {
                            UnexpectedToken(state, __FUNCTION__, token, value);
                        }
                        accumAttributes << " " << name << "=\"" << value << "\"";
                        state = STATE_START;
                    }
                    else {
                        UnexpectedToken(state, __FUNCTION__, token, value);
                    }
                    break;

                case TOKEN_COLON:
                    if (state != STATE_READCOLON) {
                        UnexpectedToken(state, __FUNCTION__, token, value);
                    }
                    state = STATE_READVALUE;
                    break;

                case TOKEN_COMMA:
                    if (state != STATE_START) {
                        UnexpectedToken(state, __FUNCTION__, token, value);
                    }
                    state = STATE_START; // commas are not informative in our implementation, and we don't check for misplaced commas
                    break;

                case TOKEN_CLOSEBRACE:
                    if (state != STATE_START) {
                        UnexpectedToken(state, __FUNCTION__, token, value);
                    }
                    break;

                case TOKEN_LINEFEED:
                    break;

                case TOKEN_EOF:
                    throw unexpected_eof_exception();

                default:
                    UnexpectedToken(state, __FUNCTION__, token, value);
            }
        }
        return std::move(accumAttributes.str());
    }

    // Arrays get written as a sequence of tags, in the same way that an object is written (in XML, tags can be repeated)
    void ParseArray(std::string repeatedName)
    {
        size_t elementNumber = 0; // used when repeated name is empty
        States state = STATE_ARRAY;
        std::string name, value;
        for (JsonToken token = TOKEN_NONE; token != TOKEN_CLOSEBRACKET;) {
            std::string elt(repeatedName.size() ? MakeTag(repeatedName) : std::to_string(elementNumber));
            token = GetNextToken(value);
            switch (token) {
                case TOKEN_VALUE:
                case TOKEN_STRING:
                    m_xml->AddData(MakeTag(elt), value);
                    elementNumber++;
                    break;

                case TOKEN_OPENBRACE:
                    ParseObject(elt);
                    elementNumber++;
                    break;

                case TOKEN_OPENBRACKET: {
                    XmlTag tag(*m_xml, elt);
                    elementNumber++;
                    ParseArray("");
                    break;
                }

                case TOKEN_CLOSEBRACKET:
                    break; // done

                case TOKEN_COMMA:
                    break; // commas are not informative in our implementation, and we don't check for misplaced commas

                case TOKEN_LINEFEED:
                    break;

                case TOKEN_EOF:
                    throw unexpected_eof_exception();

                default:
                    UnexpectedToken(state, __FUNCTION__, token, value);
            }
        }
    }

    std::string MakeTag(const std::string& str) 
    {
        std::string tag(str);
        if (!tag.empty()) {
            // deal with xml-unfriendly tags
            if (strchr("</!?", tag[0]) != nullptr) {
                tag = "\"" + tag + "\"";
            }
        }
        return std::move(tag);
    }

private:
	// Allow the input to possibly be Xml or Log (log4j format) instead of Json. We will buffer until
	// we abandon that possibility by seeing that the first token is not a less-than or left-bracket character; 
	bool m_possiblyXml;
	bool m_possiblyLog;
	std::string m_topTag;
	std::string m_backBuffer;
	std::queue<std::pair<JsonToken, std::string>> m_ungetTokens;

	// data
	std::shared_ptr<XmlOutput> m_xml;
	std::shared_ptr<XmlTag> m_topXmlTag;
	bool m_leaveOuterTagOpen;
	std::istream* m_in; // lives during scope of Read only
	char m_lastChar;
	char m_currChar;
	size_t m_charCount;
	size_t m_lineCount;
	std::string m_recallBuffer;
};

} // namespace StreamingXml
