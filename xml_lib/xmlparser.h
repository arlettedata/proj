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

#include <algorithm>
#include <functional>
#include <iostream>
#include <sstream>
#include <stack>
#include <string>
#include <vector>

#include "xmlmatcher.h"
#include "xmlquery.h"

namespace StreamingXml
{

inline bool _isspace(char ch) // avoid library call
{
    return (ch == ' ') || (ch == '\t');
}

typedef std::pair<XmlRow*, size_t> RowRef;

class XmlParser
{
    class Input
    {
    public:
        Input(std::istream* stream, bool autoDelete)
            : m_stream(stream)
            , m_charsRead(0)
            , m_autoDelete(autoDelete)
        {
        }

        ~Input()
        {
            if (m_autoDelete) {
                delete m_stream;
            }
        }

        std::istream* m_stream;
        std::streamsize m_charsRead;

    private:
        bool m_autoDelete;
    };

    class ScanContext
    {
    public:
        ScanContext(const char*& startPos)
            : pos(startPos)
            , tagBeg(nullptr)
            , tagEnd(nullptr)
            , labelBeg(nullptr)
            , labelEnd(nullptr)
            , selfTerminating(false)
        {
        }

        void reset()
        {
            tagBeg = tagEnd = labelBeg = labelEnd = 0;
        }

        size_t tagLen() const
        {
            return tagEnd - tagBeg;
        }

        size_t labelLen() const
        {
            return labelEnd - labelBeg;
        }

        const char*& pos; // continually updated
        const char* tagBeg;
        const char* tagEnd;
        const char* labelBeg;
        const char* labelEnd;
        bool selfTerminating;

    private:
        ScanContext(const ScanContext&);
        ScanContext& operator=(const ScanContext&);
    };  

public:
    enum Flags
    {
        All = 0x1,
        FoundRootNode = 0x2,
    };

    // Following must be at least the size of the longest tag, including attributes
    // Symptom of setting too low: the error "either input is not an XML file or an XML tag exceeds N characters"
    enum
    {
        DefaultBufferSize = 65536
    };

    XmlParser()
        : m_flags(0)
        , m_querySpec(new XmlQuerySpec)
        , m_context(new XmlParserContext)
        , m_totalBufferSize(DefaultBufferSize)
        , m_currDepth(0)
        , m_echoOutput(std::cout)
    {
        m_query.reset(new XmlQuery(m_context, m_querySpec));
        m_buffer = new char[(size_t)m_totalBufferSize + 1]; // Note: this puts a bound on m_totalBufferSize
        m_currentPos = m_buffer;
        InitParseState();
    }

    ~XmlParser()
    {
        delete[] m_buffer;
        while (!m_inputStack.empty()) {
            delete m_inputStack.back();
            m_inputStack.pop_back();
        }
    }

    std::vector<XmlPassType> GetPassTypes() const
    {
        std::vector<XmlPassType> passTypes;
        if (m_querySpec->IsFlagSet(XmlQuerySpec::GatherDataPassRequired) || 
            m_query->GetPivoter().RequirePrepass()) {
            passTypes.push_back(XmlPassType::GatherDataPass);
        }
        passTypes.push_back(XmlPassType::MainPass);
        if (!m_query->Streaming()) {
            passTypes.push_back(XmlPassType::StoredValuesPass);
        }
        return std::move(passTypes);
    }

    // To set the current pass type, use Reset()
    XmlPassType GetCurrentPassType() const
    {
        return m_context->passType;
    }

    void SetFlags(unsigned int flags, bool set = true)
    {
        m_flags = set ? (m_flags | flags) : (m_flags & ~flags);
    }

    bool IsFlagSet(unsigned int flag) const
    {
        return ((m_flags & flag) != 0);
    }

    const XmlQuerySpecPtr GetQuerySpec() const
    {
        return m_querySpec;
    }

    void SetRowCallback(RowCallback rowCallback)
    {
        m_query->SetRowCallback(rowCallback);
    }

    void SetIndexedJoin(XmlIndexedRows&& indexedJoin) 
    {
        m_query->SetIndexedJoin(std::move(indexedJoin));
    }

    void UngetString(const std::string& str)
    {
        m_ungetString = str;
    }

    void AddColumn(const std::string& columnSpec)
    {
        assert(!m_querySpec->IsFlagSet(XmlQuerySpec::ColumnsAdded));
        m_columnSpecs.push_back(columnSpec);
    }

    void Reset(XmlPassType passType)
    {
        assert(passType != XmlPassType::PassNotSet);
        assert(m_querySpec->IsFlagSet(XmlQuerySpec::ColumnsAdded)); // FinishColumns called?
        InitParseState();
        ResetPathMatching();
        m_context->Reset(passType);
        m_query->Reset(passType, GetPassTypes().back());
        if (m_querySpec->GetRootNodeNum() == 0) {
            SetFlags(FoundRootNode, true);
        }
    }

    XmlPivoter& GetPivoter() 
    {
        return m_query->GetPivoter();
    }

    const XmlColumns& GetColumns() const
    {
        return m_querySpec->GetColumns();
    }

    // Note: valueIdx is not the column index. Instead, it's column->valueIdx.
    const XmlValue& GetValue(size_t rowIdx, size_t valueIdx) const
    {
        assert(valueIdx != -1);
        const XmlRow& row = m_query->GetRow(rowIdx);
        return row[valueIdx];
    }

    size_t GetRowRepeatCount(size_t rowIdx) const
    {
        return m_query->GetRowRepeatCount(rowIdx);
    }

    const XmlRow& GetRow(size_t rowIdx) const 
    {
        return m_query->GetRow(rowIdx);
    }

    void CheckUnreferenced() const
    {
        for (auto& path : m_paths) {
            path->CheckUnreferenced();
        }
        m_query->CheckUnreferenced();
    }

    void StopParse()
    {
        m_query->SetFlags(XmlQuery::ParseStopped);
    }

    void PushInput(const std::string& input)
    {
        if (!input.empty()) {
            PushInput(new std::stringstream(input), true);
        }
    }

    void PushInput(std::istream* inputStream, bool autoDelete = true)
    {
        assert(inputStream->good());
        m_inputStack.push_back(new Input(inputStream, autoDelete));
    }
    
    void FinishColumns()
    {
        assert(!m_querySpec->IsFlagSet(XmlQuerySpec::ColumnsAdded)); // call once
        m_querySpec->ParseColumnSpecs(m_columnSpecs, m_query->GetPivoter());
    }

    void Parse(std::istream& stream)
    {
        assert(m_querySpec->IsFlagSet(XmlQuerySpec::ColumnsAdded)); // FinishColumns called?

        if (m_context->passType == XmlPassType::PassNotSet) {
            Reset(XmlPassType::MainPass);
        }

        PushInput(&stream, false);

        if (!m_ungetString.empty()) {
            PushInput(m_ungetString);
            m_ungetString.clear();
        }

        // Note: the XML format is contained in class ScanContext and the Scan() method, making this the starting point
        // accepting different input formats (XML, JSON, etc).  For binary stream input, e.g. metrics data,
        // ReadMoreChars() needs to be generalized to binary input.
        ScanContext sc(/*ref*/ m_currentPos);

        size_t rootNodeNum = m_querySpec->GetRootNodeNum();

        bool exit = false;
        while (!exit) {
            m_currentPos = ReadMoreChars(m_currentPos, /*out*/ exit);
            while (!exit && Scan(sc)) {
                const char* tag = sc.labelBeg;
                size_t len = sc.labelLen(); // length excluding brackets
                size_t redirectLen = sc.tagLen(); // length including brackets
                bool startTag = (*tag != '/') && (*tag != '?') && (*tag != '!');
                bool endTag = (*tag == '/');

                if (startTag) {
                    m_context->numNodes++;

                    if (rootNodeNum && !IsFlagSet(FoundRootNode) && (m_context->numNodes == rootNodeNum)) {
                        SetFlags(FoundRootNode, true);
                    }

                    if (!IsFlagSet(FoundRootNode)) {
                        continue;
                    }

                    m_currDepth++;

                    if (m_querySpec->IsFlagSet(XmlQuerySpec::NodeStackRequired)) {
                        m_context->nodeStack.push_back(XmlNodeInfo(tag, len, m_context->numNodes));
                    }

                    if (m_context->appendingValues) {
                        Redirect(sc.tagBeg, redirectLen, AppendValues);
                        m_context->appendingValues = false;
                    }

                    m_matcher->MatchStartTag(tag, len);

                    if (m_matcher->GetMatchType() == XmlMatcher::MatchType::AllMatchedWithNoDataMatches) { 
                        // e.g. attribute matches (we don't care about bar in <foo a="x">bar</foo>
                        m_matcher->CommitMatch();
                        m_query->EmitRow();
                    }

                    if (sc.selfTerminating) { // e.g. <foo a="x"/> and not <foo></foo>
                        PopAttributes();
                        goto EndTag;
                    }
                }
                else if (endTag) {
                    // Remove slash
                    tag++;
                    len--;
                    
                EndTag:
                    if (!IsFlagSet(FoundRootNode)) {
                        continue;
                    }

                    if (--m_currDepth == 0 && rootNodeNum) {
                        SetFlags(FoundRootNode, false);
                        m_query->SetFlags(XmlQuery::ParseStopped, true);
                    }

                    m_query->OnEndTag(m_currDepth);

                    m_context->appendingValues = false;

                    bool matchedEndTag = m_matcher->MatchEndTag(tag, len);

                    if (m_context->appendingValues) {
                        Redirect(sc.tagBeg, redirectLen, AppendValues);
                    }

                    if (matchedEndTag) {
                        Redirect(sc.tagBeg, redirectLen, Echo);
                        redirectLen = 0;

                        if (m_matcher->GetMatchType() == XmlMatcher::MatchType::AllMatched) { 
                            // as opposed to AllMatchedWithNoDataMatches (see comment above)
                            m_matcher->CommitMatch();
                            m_query->EmitRow();
                        }
                    }

                    if (m_querySpec->IsFlagSet(XmlQuerySpec::NodeStackRequired) && m_context->nodeStack.size()) {
                        m_context->nodeStack.pop_back();
                    }

                    if (ControlCIssued()) {
                        m_query->SetFlags(XmlQuery::ParseStopped);
                    }

                    if (m_query->IsFlagSet(XmlQuery::ParseStopped)) {
                        exit = true;
                    }
                }

                Redirect(sc.tagBeg, redirectLen, Echo);
            }
        }
    }

    void OutputStoredRows()
    {
        Reset(XmlPassType::StoredValuesPass);
        m_query->OutputStoredRows();
    }

private:
    // Note: the following initializes the parse state, but doesn't initialize result of processing column args; see
    // ProcessPaths
    void InitParseState()
    {
        SetFlags(FoundRootNode, (m_querySpec->GetRootNodeNum() == 0));
        m_usedBufferSize = 0;
        m_unusedBufferSize = m_totalBufferSize;
        while (!m_inputStack.empty()) {
            delete m_inputStack.back();
            m_inputStack.pop_back();
        }
        m_tagState = NotInTag;
        m_currentPos = m_buffer;
    }

    void ResetPathMatching() 
    {
        // Create paths that we will query in this path using the pathrefs 
        // we have recorded in XmlQuerySpec.  We exclude the joined path refs,
        // since that is done in a separate pass.
        m_paths.clear();
        auto& pathRefs = m_querySpec->GetInputSpec().pathRefs;
        for (auto it = pathRefs.begin(); it != pathRefs.end(); it++) {
            auto& pathRef = it->second;
            XmlPathPtr path(new XmlPath(m_context, pathRef));
            m_paths.push_back(path);
            it->second->path = path;
        }
        m_matcher.reset(new XmlMatcher(m_context, m_paths));
    }

    // Note: this assumes nothing more than the input being a series of characters,
    // but could conceivably be generalized for binary input.
    const char* ReadMoreChars(const char* pos, bool& reachedEndOfStream)
    {
        reachedEndOfStream = false;

        //
        // Move consumed characters off the m_buffer by shifting left,
        // determine number of characters that is left to be read into
        // the m_buffer.
        //
        std::streamsize readlen;
        if (pos > m_buffer) {
            std::streamsize consumedSize = pos - m_buffer;
            std::streamsize unconsumedSize = m_totalBufferSize - consumedSize - m_unusedBufferSize;
            assert(consumedSize + unconsumedSize + m_unusedBufferSize == m_totalBufferSize);
            readlen = m_totalBufferSize - unconsumedSize;
            memmove(const_cast<char*>(m_buffer), pos, (size_t)unconsumedSize);
            pos = m_buffer + unconsumedSize; // point pos to where we will read in more chars
            *const_cast<char*>(pos) = '\0';
            m_usedBufferSize = unconsumedSize;
        }
        else {
            assert(pos == m_buffer);
            readlen = m_totalBufferSize;
            m_usedBufferSize = 0;
        }

        while ((m_usedBufferSize < m_totalBufferSize) && !m_inputStack.empty()) {
            std::istream& stream = *m_inputStack.back()->m_stream;
            if (stream.good()) {
                stream.read(
                    const_cast<char*>(pos), (std::streamsize)readlen); // read one less so we can add a null character

                std::streamsize size = stream.gcount();
                m_inputStack.back()->m_charsRead += size;

                // remove trailing null characters
                while ((size > 0) && (*(pos + size - 1) == '\0')) {
                    size--;
                }

                readlen -= size;
                m_usedBufferSize += size;
                pos += size;

                assert(m_usedBufferSize <= m_totalBufferSize); // dont overflow!
            }
            else {
                // stream is exhausted, so we can pop now
                delete m_inputStack.back();
                m_inputStack.pop_back();
            }
        }

        m_unusedBufferSize = readlen; // remember portion of the m_buffer that is unused

        assert(pos == m_buffer + m_usedBufferSize); // pos and m_usedBufferSize should agree
        assert(m_usedBufferSize + m_unusedBufferSize == m_totalBufferSize); // filled part + empty part = m_buffer size
        assert(!m_unusedBufferSize || m_inputStack.empty()); // having more room for chars implies all input was exhausted

        const_cast<char*>(m_buffer)[m_usedBufferSize] = '\0'; // null terminate the used portion of the m_buffer
        pos = m_buffer; // set pos at the beginning of the m_buffer (which is the new consumption point)

        if (!m_usedBufferSize) {
            reachedEndOfStream = true;
        }

        return pos;
    }

    bool Scan(ScanContext& sc)
    {
        sc.reset();

        int linesFound = 0;
        auto scan_to_char = [&](const char* scanPos, char ch) -> const char* {
            if (m_querySpec->IsFlagSet(XmlQuerySpec::LineNumUsed)) {
                for (; *scanPos != ch; scanPos++) {
                    if (*scanPos == '\n') {
                        linesFound++;
                    }
                    else if (*scanPos == '\0') {
                        return 0;
                    }
                }
            }
            else {
                scanPos = strchr(scanPos, ch);
            }
            return scanPos;
        };

        sc.tagBeg = scan_to_char(sc.pos, '<');
        if (!sc.tagBeg) { // no beginning of tag found
            if (sc.pos == m_buffer) { // if the m_buffer is currently full, then flush
                Redirect(sc.pos, (size_t)m_usedBufferSize, Echo | 
                    (m_context->appendingValues ? AppendValues : 0)); // truncation intended (don't expect m_usedBufferSize to exceed size_t)
                m_context->numLines += linesFound; // count in lines we found since we're eating all the characters now
                sc.pos += m_usedBufferSize;
            }
            return false; // otherwise, we can read in more characters, so leave things alone and retry
        }

        if (sc.pos != sc.tagBeg) {
            Redirect(sc.pos, sc.tagBeg - sc.pos, Echo | (m_context->appendingValues ? AppendValues : 0));
        }
        m_context->numLines += linesFound; // count in lines we found up to the '<'
        linesFound = 0; // reset for more line counting
        sc.pos = (char*)sc.tagBeg;

        sc.tagEnd = scan_to_char(sc.pos + 1, '>');
        if (!sc.tagEnd) { // no end of tag found
            if (sc.pos == m_buffer) {
                // if we are at the beginning of the m_buffer, then this condition indicates that the m_buffer size is
                // too small. (This is an extreme edge case.)
                XmlUtils::Error("Either input is not an XML file or an XML tag exceeds %s characters.",
                    XmlUtils::ToString((__int64_t)m_totalBufferSize));
            }
            // Don't account for line numbers.  Just return false, pull in more characters, and retry.
            return false;
        }

        m_context->numLines += linesFound; // count in lines we found up to the '>'
        sc.tagEnd++; // point past '>'
        sc.pos = (char*)sc.tagEnd; // advance parser

        sc.labelBeg = sc.tagBeg + 1; // point to first character after '<'
        sc.labelEnd = sc.tagEnd - 2; // point to first character before '>'
        
        bool malformed = false;

        // trim whitespace
        while (_isspace(*sc.labelBeg) && (sc.labelBeg != sc.labelEnd + 1)) {
            sc.labelBeg++;
        }
        if (sc.labelBeg == sc.labelEnd + 1) {
            malformed = true;
        }
        while (_isspace(*sc.labelBeg) && (sc.labelEnd != sc.labelBeg - 1)) {
            sc.labelEnd--;
        }
        sc.selfTerminating = (*sc.labelEnd == '/');
        if (sc.selfTerminating) {
            sc.labelEnd--;
        }
        if (sc.labelEnd == sc.labelBeg - 1) {
            malformed = true;
        }

        // End tag is the signal for popping attributes
        bool popAttributes = *sc.labelBeg == '/'; // end

        // Look for presence of attributes; see note about <foo a="b"/> above.
        bool pushAttributes = false;
        if (!malformed && !popAttributes /*&& !sc.selfTerminating*/) {
            const char* pos = sc.labelEnd;
            while (_isspace(*pos) && pos != sc.labelBeg) {
                pos--;
            }
            for (; pos != sc.labelBeg && !pushAttributes; pos--) {
                if (*pos == '\"' || *pos == '\'') {
                    pushAttributes = true;
                }
                else if (!_isspace(*pos)) {
                    break;
                }
            }
        }

        sc.labelEnd++; // point one character past last character of label

        // Special tags don't contain attributes
        if (*sc.labelBeg == '?' || *sc.labelBeg == '!') {
            return true;
        }

        bool useAttributes = m_querySpec->IsFlagSet(XmlQuerySpec::AttributesUsed);

        if (pushAttributes) {
            std::vector<std::string> words = std::move(XmlUtils::Split(std::string(sc.labelBeg, 
                sc.labelEnd - sc.labelBeg), " ", "\""));
            if (useAttributes) {
                int attrCnt = 0;
                for (size_t i = 1; i < words.size(); i++) { // skip tag
                    const std::string& word = words[i];
                    std::vector<std::string> tokens = std::move(XmlUtils::Split(word, "="));
                    if (tokens.size() != 2) {
                        malformed = true;
                    }
                    else {
                        const std::string& name = tokens[0];
                        const std::string& value = tokens[1];
                        if (!(value.front() == '\"' && value.back() == '\"') &&
                            !(value.front() == '\'' && value.back() == '\'')) {
                            malformed = true;
                        }
                        else {
                            std::string trimmedValue(value, 1, value.size() - 2);
                            m_context->attrStack.push_back(make_pair(name, trimmedValue));
                            attrCnt++;
                        }
                    }
                    if (malformed) {
                        break;
                    }
                }
                m_context->attrCountStack.push_back(attrCnt);
            }

            // reset labelEnd to exclude attributes
            if (words.size()) {
                sc.labelEnd = sc.labelBeg + words[0].size();
            }
        }
        else if (popAttributes) {
            PopAttributes();
        }
        else {
            m_context->attrCountStack.push_back(0);
        }

        if (malformed) {
            XmlUtils::Error("Invalid XML tag: %s", 
                std::string(sc.tagBeg, (size_t)sc.tagEnd - (size_t)sc.tagBeg));
        }

        return true;
    }

    void PopAttributes()
    {
        if (m_context->attrCountStack.size()) {
            int cnt = m_context->attrCountStack.back();
            m_context->attrCountStack.pop_back();
            while (cnt-- > 0 && m_context->attrStack.size()) {
                m_context->attrStack.pop_back();
            }
        }
    }

    enum RedirectFlags
    {
        AppendValues = 0x1,
        Echo = 0x2
    };

    void Redirect(const std::string& str, unsigned int redirFlags)
    {
        Redirect(str.c_str(), str.length(), redirFlags);
    }

    void Redirect(const char* pos, size_t len, unsigned int redirFlags)
    {
        if (redirFlags & AppendValues) {
            for (auto& path : m_paths) {
                path->AppendValue(pos, len);
            }
        }

        //
        // Write to output (cout or other stream given by default)
        //
        if ((redirFlags & Echo) && IsFlagSet(All)) {
            m_echoOutput.write(pos, (std::streamsize)len);
        }
    }

    unsigned int m_flags;
    std::vector<std::string> m_columnSpecs;
    XmlParserContextPtr m_context;
    XmlQuerySpecPtr m_querySpec;
    std::unique_ptr<XmlQuery> m_query;
    std::unique_ptr<XmlMatcher> m_matcher;
    XmlPaths m_paths;
    std::string m_ungetString;
    std::vector<Input*> m_inputStack;
    const char* m_buffer;
    const char* m_currentPos;
    std::streamsize m_totalBufferSize;
    std::streamsize m_unusedBufferSize;
    std::streamsize m_usedBufferSize;
    int m_currDepth;

    // Output
    std::ostream& m_echoOutput;
    enum
    {
        NotInTag,
        FoundLeftBracket,
        InStartTag,
        InEndTag
    } m_tagState;
    
    ControlCHandler m_controlChandler;

    XmlParser(const XmlParser&); // copy constructor not implemented
    const XmlParser& operator=(const XmlParser&); // assignment operator not implemented
};

} // namespace StreamingXml
