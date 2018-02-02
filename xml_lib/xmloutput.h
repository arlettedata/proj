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
#include <fstream>
#include <functional>
#include <iostream>
#include <ostream>
#include <sstream>
#include <stack>
#include <stdlib.h>
#include <string>
#include <vector>

namespace StreamingXml
{

class XmlOutput
{
public:
    enum Flags
    {
        NoWhitespace = 0,
        NewLines = 1,
        Indents = 2
    };
    XmlOutput(std::ostream* outputStream = &std::cout, unsigned int flags = NewLines | Indents)
        : m_flags(flags)
    {
        m_verbosity = All;
        m_indentLevel = 0;
        m_prevStream = 0;
        m_outputStream = 0;
        Attach(outputStream);
    }

    XmlOutput(std::ostream& outputStream, unsigned int flags = NewLines | Indents)
        : m_flags(flags)
    {
        m_verbosity = All;
        m_indentLevel = 0;
        m_prevStream = 0;
        m_outputStream = 0;
        Attach(&outputStream);
    }

    virtual ~XmlOutput()
    {
        CloseFile();
    }

    int SetVerbosity(int verbosity)
    {
        int prevVerbosity = m_verbosity;
        m_verbosity = verbosity;
        return prevVerbosity;
    }

    int GetVerbosity() const
    {
        return m_verbosity;
    }

    enum Verbosities
    {
        Disabled = -1,
        All = 0
    };

    bool OpenFile(const std::string& fileName)
    {
        assert((m_prevStream == 0) || !"Can't call OpenFile() twice");
        assert(!fileName.empty());
        std::ostream* newStream = new std::ofstream(fileName.c_str());
        if (newStream->fail()) {
            assert(!"failed to OpenFile");
            delete newStream;
            return false;
        }
        Attach(newStream);
        return true;
    }

    void CloseFile()
    {
        if (m_prevStream) {
            Flush();
            // Detach ...
            std::ofstream* openStream = (std::ofstream*)m_outputStream;
            m_outputStream = m_prevStream;
            m_prevStream = 0;
            if (openStream) {
                openStream->close();
                delete openStream;
            }
        }
    }

    void SetPopTagCallback(std::function<void()> callback)
    {
        m_popTagCallback = callback;
    }

    void SetFlags(unsigned int flags)
    {
        m_flags = flags;
    }

    std::ostream& GetStream() const
    {
        assert(m_outputStream);
        return *m_outputStream;
    }

    int SetIndentLevel(int _indentLevel)
    {
        int _prevIndentLevel = m_indentLevel;
        m_indentLevel = _indentLevel;
        return _prevIndentLevel;
    }

    void PushTag(const std::string& tag, int verbosity = All, const std::string& attributes = std::string())
    {
        if (!IsAttached() || (verbosity > m_verbosity)) {
            return;
        }
        Indent(verbosity);
        m_indentLevel++;
        *m_outputStream << "<" << tag + attributes << ">";
        NewLine(verbosity);
        m_tagStack.push(tag);
    }

    void AddSelfTerminatedTag(const std::string& tag, const std::string& attributes, int verbosity = All)
    {
        if (!IsAttached() || (verbosity > m_verbosity)) {
            return;
        }
        Indent(verbosity);
        m_indentLevel++;
        *m_outputStream << "<" << tag + attributes << "/>";
        m_indentLevel--;
        Indent(verbosity);
        NewLine(verbosity);
    }

    void PopTag(int verbosity = All)
    {
        if (!IsAttached() || (verbosity > m_verbosity)) {
            return;
        }
        m_indentLevel--;
        Indent(verbosity);

        assert(!m_tagStack.empty());
        if (m_tagStack.top().size()) {
            *m_outputStream << "</" << m_tagStack.top() << ">";
        }
        NewLine(verbosity);
        m_tagStack.pop();
        if (m_popTagCallback) {
            m_popTagCallback();
        }
    }

    template <class T>
    void AddData(const std::string& tag, const T& data, int verbosity = All, bool DataOnSeparateLine = false)
    {
        if (!IsAttached() || (verbosity > m_verbosity)) {
            return;
        }
        if (DataOnSeparateLine) {
            Indent(verbosity);
            *m_outputStream << "<" << tag << ">";
            NewLine(verbosity);
            Indent(verbosity);
            if (m_flags & Indents) {
                *m_outputStream << "  ";
            }
            Encode(data);
            NewLine(verbosity);
            Indent(verbosity);
            *m_outputStream << "</" << tag << ">";
            NewLine(verbosity);
        }
        else {
            Indent(verbosity);
            *m_outputStream << "<" << tag << ">";
            Encode(data);
            *m_outputStream << "</" << tag << ">";
            NewLine(verbosity);
        }
    }

    template <class T>
    void AddData(const std::string& tag, const std::vector<T>& data, int verbosity = All, bool DataOnSeparateLine = false)
    {
        if (!IsAttached() || (verbosity > m_verbosity)) {
            return;
        }
        if (DataOnSeparateLine) {
            Indent(verbosity);
            *m_outputStream << "<" << tag << ">";
            NewLine(verbosity);
            Indent(verbosity);
            if (m_flags & Indents) {
                *m_outputStream << "  ";
            }
        }
        else {
            Indent(verbosity);
            *m_outputStream << "<" << tag << ">";
        }

        for (int i = 0; i < (int)data.size(); i++) {
            if (i > 0) {
                *m_outputStream << ",";
            }
            *m_outputStream << data[i];
        }

        if (DataOnSeparateLine) {
            NewLine(verbosity);
            Indent(verbosity);
            *m_outputStream << "</" << tag << ">";
            NewLine(verbosity);
        }
        else {
            *m_outputStream << "</" << tag << ">";
            NewLine(verbosity);
        }
    }

    void Indent(int verbosity = All)
    {
        if (!IsAttached() || !(m_flags & Indents) || (verbosity > m_verbosity)) {
            return;
        }
        for (int i = 0; i < m_indentLevel; i++) {
            *m_outputStream << "  ";
        }
    }

    void NewLine(int verbosity = All)
    {
        if (!IsAttached() || !(m_flags & NewLines) || (verbosity > m_verbosity)) {
            return;
        }
        *m_outputStream << std::endl;
    }

    virtual bool IsNullOutput() const
    {
        return false;
    }

    void Flush()
    {
        if (!IsAttached()) {
            return;
        }
        m_outputStream->flush();
    }

    void AddNullTerminator() // do this for strstream outputs
    {
        m_outputStream->write("", 1);
    }

    void Attach(std::ostream* _outputStream)
    {
        assert((m_prevStream == 0) || !"Can't call Attach() while a file is open");
        m_prevStream = m_outputStream;
        m_outputStream = _outputStream;
    }

    bool IsAttached() const
    {
        return (m_outputStream != 0);
    }

    std::ostream* Detach()
    {
        assert((m_prevStream == 0) || !"Can't call Detach() while a file is open");
        Flush();
        std::ostream* prevOutput = m_outputStream;
        m_outputStream = 0;
        return prevOutput;
    }

private:
    void Encode(bool data) 
    {
        *m_outputStream << (data ? "true" : "false");
    }

    void Encode(const char* data, bool justBrackets = true)
    {
        std::ostream& out = *m_outputStream;
        if (justBrackets) {
            for (const char* pos = data; *pos != '\0'; pos++ ) {
                switch(*pos) {
                    case '<':
                        out << "&lt;";
                        break;
                    case '>':
                        out << "&gt;";
                        break;
                    default:
                        out << *pos;
                }
            }
        } 
        else {
            for (const char* pos = data; *pos != '\0'; pos++ ) {
                switch(*pos) {
                    case '&':
                        out << "&amp;";
                        break;
                    case '<':
                        out << "&lt;";
                        break;
                    case '>':
                        out << "&gt;";
                        break;
                    case '\'':
                        out << "&apos;";
                        break;
                    case '\"':
                        out << "&quot;";
                        break;
                    default:
                        out << *pos;
                }
            }
        }
    }

    void Encode(const std::string& data, bool justBrackets = true) 
    {
        Encode(data.c_str(), justBrackets);
    }

    template<typename T>
    void Encode(T data)
    {
        *m_outputStream << data;
    }

    std::ostream* m_outputStream;
    std::ostream* m_prevStream;
    std::stack<std::string> m_tagStack;
    int m_verbosity;
    int m_indentLevel;
    unsigned int m_flags;
    std::function<void()> m_popTagCallback;

    friend class XmlString;
};

class XmlString : public XmlOutput
{
public:
    XmlString(unsigned int flags = NewLines | Indents)
        : XmlOutput(m_stream, flags)
    {
    }

    std::string& GetString(std::string& str) const
    {
        if (IsAttached()) {
            ((/*unconst*/ std::ostream*)m_outputStream)->write("", 1); // null terminate
            str.assign(m_stream.rdbuf()->str());
        }
        else {
            str.assign("");
        }
        return str;
    }

private:
    std::stringstream m_stream;
};

class NullXmlOutput : public XmlString
{
public:
    bool IsNullOutput() const // override
    {
        return true;
    }
};

class XmlTag
{
public:
    XmlTag(XmlOutput& _xml, const std::string& tag, int verbosity = XmlOutput::All,
        const std::string& attributes = std::string(), bool selfTerminated = false)
        : m_xml(_xml)
        , m_verbosity(verbosity)
        , m_selfTerminated(selfTerminated)
    {
        if (m_selfTerminated) {
            m_xml.AddSelfTerminatedTag(tag, attributes, m_verbosity);
        }
        else {
            m_xml.PushTag(tag, m_verbosity, attributes);
        }
    }

    ~XmlTag()
    {
        if (!m_selfTerminated) {
            m_xml.PopTag(m_verbosity);
        }
    }

private:
    XmlOutput& m_xml;
    int m_verbosity;
    bool m_selfTerminated;
};

} // namespace StreamingXml
