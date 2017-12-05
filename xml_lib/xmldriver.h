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

#include "xmljson.h"
#include "xmloutput.h"
#include "xmlparser.h"

#include <fstream>
#include <functional>
#include <iostream>
#include <istream>
#include <queue>

namespace StreamingXml
{

class XmlDriver
{
public:
    XmlDriver()
        : m_parser(new XmlParser)
        , m_inputHeader(false)
        , m_outputHeader(false)
    {
    }

    ~XmlDriver()
    {
        if (m_input.get() == &std::cin) {
            m_input.release();
        }
        if (m_output.get() == &std::cout) {
            m_output.release();
        }
    }

    bool Initialize(int argc, char* argv[])
    {
        std::vector<std::string> columnArgs;
        for (int i = 1; i < argc; i++) {
            columnArgs.push_back(argv[i]);
        }
        ReadColumnSpecs(columnArgs);
        m_parser->FinishColumns();
        bool showUsage = Configure(); 
        return showUsage;
    }

    int Run()
    {
        XmlQuerySpecPtr querySpec = m_parser->GetQuerySpec();
        if (querySpec->IsFlagSet(XmlQuerySpec::LeftSideOfJoin)) {
            XmlDriver join;
            XmlIndexedRows indexedJoin = std::move(join.LoadIndexedJoin(querySpec->GetJoinSpec())); 
            m_parser->SetIndexedJoin(std::move(indexedJoin)); 
        }

        SetOutput();

        m_parser->SetRowCallback([&](size_t rowIdx) {
            size_t cnt = m_parser->GetRowRepeatCount(rowIdx);
            for (size_t i = 0; i < cnt; i++) {
                PrintRow(rowIdx);
            }
        });

        DoPasses();
        return 0;
    }

private:
    XmlIndexedRows LoadIndexedJoin(const XmlQuerySpec::JoinSpec& joinSpec) 
    {
        m_inputHeader = joinSpec.header;
        m_inputFilename = joinSpec.filename;

        // Initialize query spec by using the join spec from the main query spec.
        m_parser->GetQuerySpec()->AddJoinColumns(joinSpec);

        // Mark the columns that we'll index with (for where[] equality comparisons)
        std::vector<size_t> indices;
        for (auto& column : joinSpec.columns) {
            if (column->flags & XmlColumn::Indexed) {
                assert(column->index != -1); 
                indices.push_back(column->index);
            }
        }
        
        // Specify function to read the join table rows and write to an indexed join table.
        XmlIndexedRows indexedJoin;
        m_parser->SetRowCallback([&](size_t rowIdx) {
            const XmlRow& xmlRow = m_parser->GetRow(rowIdx);
            StreamingXml::XmlRowHash hash(indices);
            size_t index = hash(xmlRow);
            XmlRowsPtr xmlRows;
            auto it = indexedJoin.find(index);
            if (it != indexedJoin.end()) {
                xmlRows = it->second;
            } 
            else {
                xmlRows.reset(new XmlRows());
                indexedJoin.emplace(index, xmlRows);
                assert(indexedJoin.find(index) != indexedJoin.end());
            }
            // Now we have a bucket of rows with same hash. Considering that this 
            // index is formed by picking out the equality filters (e.g. where[right:foo==expr]),
            // it is okay to have more rows than we will actually join (i.e. false positives)
            // because the values coming from the joined table will be evaluated for equality.
            xmlRows->push_back(std::move(xmlRow));
        });
        
        // Load the join table.
        DoPasses();

        return std::move(indexedJoin);
    } 

    void ReadColumnSpecs(const std::vector<std::string>& columnArgs)
    {
        for (auto& columnArg : columnArgs) {
            // @argument-file contains additional args, space- and line-delimited. Recurse on these.
            // An alternative form is argument-file@ which allows us to auto-complete before typing '@'.
            if (columnArg[0] == '@' || columnArg.back() == '@') {
                std::string argFile = columnArg[0] == '@' 
                    ? columnArg.substr(1) 
                    : columnArg.substr(0, columnArg.size() - 1);
                if (argFile.empty()) {
                    XmlUtils::Error("Missing argument-inclusion filename after @"); /**/
                }
                std::ifstream input(argFile);
                if (input.fail()) {
                    XmlUtils::Error("Argument-inclusion filename could not be opened: %s", argFile); /**/
                }
                std::string line;
                while (XmlUtils::GetLine(input, line)) {
                    // Truncate at unquoted comment character
                    std::vector<std::string> splitAtComments = std::move(XmlUtils::Split(line, "#", "{\"'", true /*insertGaps*/));
                    if (splitAtComments.size()) {
                        ReadColumnSpecs(XmlUtils::Split(splitAtComments[0], " "));
                    }
                }
            }
            else {
                m_parser->AddColumn(columnArg);
            }
        }
    }

    bool Configure() // for the main query, not join query
    {
        XmlQuerySpecPtr querySpec = m_parser->GetQuerySpec(); 
        m_inputFilename = querySpec->GetInputSpec().filename;
        m_inputHeader = querySpec->GetInputSpec().header;
        m_outputHeader = querySpec->GetOutputSpec().header;
        if (querySpec->GetNumValueColumns() == 0 && !querySpec->IsFlagSet(XmlQuerySpec::HasPivot)) {
            m_parser->SetFlags(XmlParser::All); // if no output columns specified, then echo input xml
        }
        return querySpec->IsFlagSet(XmlQuerySpec::ShowUsage);
    }

    void DoPasses() 
    {
        auto passes = m_parser->GetPassTypes();
        for (auto& passType : passes) {
            if (ControlCIssued()) {
                break;
            }
            m_parser->Reset(passType);
            switch (passType) {
                case XmlPassType::PassNotSet:
                    break;
                case XmlPassType::GatherDataPass:
                    SetInput(true);
                    Parse();
                    break;
                case XmlPassType::MainPass:
                    SetInput(false);
                    Parse();
                    break;
                case XmlPassType::StoredValuesPass:
                    m_parser->OutputStoredRows();
                    break;
            }
        }
    }

    void Parse()
    {
        std::unique_ptr<Json2Xml> json2xml;
        std::string backBuffer;
        bool parseAsLogOrCsv = false;
        try {
            std::stringstream strm;
            std::shared_ptr<StreamingXml::XmlOutput> xml(new StreamingXml::XmlOutput(strm));
            std::function<void()> process = [&] {
                m_parser->Parse(strm);
                strm.str(std::string()); // reset
                strm.clear();
            };
            xml->SetPopTagCallback(process);
            json2xml.reset(new Json2Xml(xml, "json"));
            while (json2xml->Read(*m_input).get()) {
                process();
            }
        }
        catch (XmlInputException ex) {
            backBuffer = std::move(json2xml->GetBackBuffer()); 
            if (ex.getPossibleFormat() == "xml") {
                m_parser->UngetString(backBuffer);
                m_parser->Parse(*m_input);
            } else {
                parseAsLogOrCsv = true;
            }
        } 
        catch (std::exception&) {
            backBuffer = std::move(json2xml->GetBackBuffer()); 
            parseAsLogOrCsv = true;
        }

        if (parseAsLogOrCsv) {
            std::queue<std::string> backLines;
            if (!ParseLog(backBuffer, backLines)) {
                if (!ParseCsv(backLines)) {
                    XmlUtils::Error("Input not recognized as json, xml, csv/tsv, or log");
                }
            }
        }

        m_parser->CheckUnreferenced();
    }

    bool ParseLogLine(const std::string& line, XmlDateTime& dt, std::string& level, std::string& category, std::string& msg) 
    {
        std::vector<std::string> parts;
        std::vector<const char*> positions;
        XmlUtils::Split(line, parts, " []", "", false, &positions );
        size_t numParts = parts.size();
        size_t currPart = 0;
        char isdigit0 = numParts >= 1 && isdigit(parts[0][0]);
        char isdigit1 = numParts >= 2 && isdigit(parts[1][0]);
        dt = XmlDateTime();
        dt.error = 1;
        if (numParts == 1 && isdigit0) {
            dt = XmlDateTime::FromString(parts[currPart++]);
        } 
        else if (numParts >= 2) {
            if (isdigit0 && !isdigit1) {
                dt = XmlDateTime::FromString(parts[currPart++]);
            } 
            else if (isdigit0 && isdigit1) {
                dt = XmlDateTime::FromString(parts[currPart], parts[currPart+1]);
                currPart += 2;
            }
        }
        if (currPart < numParts) {
            level = parts[currPart++];
        } 
        else {
            level.clear();
        }
        // categories, when they exist, are separated before msg with a " - ", so it should show up as a part.
        if (currPart + 1 < numParts && parts[currPart + 1] == "-") {
            category = parts[currPart];
            currPart += 2;
        }
        else {
            category.clear();
        }
        if (currPart < numParts) {
            msg = positions[currPart];
        }
        else {
            msg.clear();
        }
        return !dt.error; // level, category, and msg can be empty and we'll still accept the log line
    }

    bool ParseLog(std::string& backBuffer, std::queue<std::string>& backLines)
    {
        std::stringstream strm;
        std::shared_ptr<StreamingXml::XmlOutput> xml(new StreamingXml::XmlOutput(strm));
        std::stack<std::shared_ptr<StreamingXml::XmlTag>> openedTags;
        XmlDateTime next_dt;
        next_dt.error = 1;
        std::string line,next_level, next_category, next_msg;

        // scan until we find a log line
        int numLinesToSeek = 10; // allow 9 non-log lines before we pick up the first log line
        while (numLinesToSeek-- > 0 && XmlUtils::GetLine(*m_input, line)) {
            if (!backBuffer.empty()) {
                line = backBuffer + line;
                backBuffer.clear();
            }
            if (line.empty() && m_input->eof()) {
                break;
            }
            if (ParseLogLine(line, next_dt, next_level, next_category, next_msg)) {
                break;
            }
            backLines.push(std::move(line));   
        }
        if (next_dt.error || numLinesToSeek == 0) {
            return false;
        }
            
        while (!m_input->eof()) {
            XmlDateTime dt = next_dt;
            std::string level = std::move(next_level);
            std::string category = std::move(next_category);
            std::string msg = std::move(next_msg);

            // Append non-log lines to msg. If we see a log line, jump to top of log line parser loop.
            while (XmlUtils::GetLine(*m_input, line)) {
                if (ParseLogLine(line, next_dt, next_level, next_category, next_msg)) {
                    break;
                }
                msg += "\n";
                msg += line;
            }

            bool handled = false;
            if (level == "TRACE") {
                if (category == "START") {
                    openedTags.push(ParseEmbeddedJson(xml, msg, true)); // this will leave the outer tag open
                    handled = true;
                }
                else if (category == "END") {
                    openedTags.pop(); // this will close the tag
                    handled = true;
                }
                else if (category == "ROOT") {
                    // to deal with unbalanced START/END (a bug), logs have a safeguard where we expect to be at zero
                    // depth
                    while (openedTags.size()) {
                        openedTags.pop();
                    }
                    handled = true;
                }
            }

            if (!handled) {
                // Transform to xml row
                xml->PushTag("log");
                xml->AddData("time", dt.ToString()); // will be empty if dt.error is set
                xml->AddData("level", level);
                xml->AddData("category", category);
                ParseEmbeddedJson(xml, msg);
                // escape backslashes before writing to xml
                XmlUtils::Replace(msg, "\\", "\\\\");
                xml->AddData("msg", XmlUtils::TrimWhitespace(msg));
                xml->PopTag();
            }

            // Parse the row
            m_parser->Parse(strm);
            strm.str(std::string()); // reset
            strm.clear();
        }
        
        return true;
    }

    void ParseCsvLine(const std::string& line, const std::string& delimiter, std::vector<std::string>& parts)
    {
        XmlUtils::Unquote(XmlUtils::Split(line, parts, delimiter, "\"", true /*insertGaps*/));
    }

    bool ParseCsv(std::queue<std::string>& backLines) // backBuffer is from the log parse rejection and has lines
    {
        std::stringstream strm;
        std::shared_ptr<StreamingXml::XmlOutput> xml(new StreamingXml::XmlOutput(strm));
        std::stack<std::shared_ptr<StreamingXml::XmlTag>> openedTags;

        auto getNextLine = [&backLines, this](std::string& line) -> bool {
            if (!backLines.empty()) {
                line = std::move(backLines.front());
                backLines.pop();
                return true;
            }
            return XmlUtils::GetLine(*this->m_input, line);
        };

        std::string firstLine;
        if (!getNextLine(firstLine)) {
            return false;
        }
        XmlUtils::TrimTrailingWhitespace(firstLine);
        
        std::string delimiter = "\t";
        std::vector<std::string> firstLineParts;
        ParseCsvLine(firstLine, delimiter, firstLineParts);
        if (firstLineParts.size() < 2) {
            delimiter = ",";
            ParseCsvLine(firstLine, delimiter, firstLineParts);
        }

        std::vector<std::string> fieldNames;
        if (m_inputHeader) {
            fieldNames = firstLineParts;
            for (auto& fieldName : fieldNames) {
                // deal with xml-unfriendly field names
                if (strchr("</!?", fieldName[0]) != nullptr) {
                    fieldName = "\"" + fieldName + "\"";
                }
                std::replace(fieldName.begin(), fieldName.end(), '\t', '_');
                std::replace(fieldName.begin(), fieldName.end(), '\n', '_');
                std::replace(fieldName.begin(), fieldName.end(), '\r', '_');
            }
            firstLineParts.clear();
            firstLine.clear();
        }

        xml->PushTag("table");
        std::string line;
        std::vector<std::string> values;
        while (true) {
            // keep reading line parts until we are free of quoted newlines. Also skip blank lines.
            line.clear();
            std::string linePart;
            bool eof = false;
            bool inQuotes = false;
            do {
                if (firstLine.size()) {
                    linePart = firstLine;
                    firstLine.clear();
                }
                else {
                    if (!getNextLine(linePart)) {
                        eof = true;
                        break;
                    }
                }
                for (size_t i = 0; i < linePart.size(); i++) {
                    if (linePart[i] == '\\') {
                        i++;
                    }
                    else if (linePart[i] == '\"') {
                        inQuotes = !inQuotes;
                    }
                }
                line += linePart;
                if (inQuotes) {
                    line += "\n";
                }
            } while(inQuotes);
            if (eof) {
                break;
            }
            XmlUtils::TrimTrailingWhitespace(line);
            ParseCsvLine(line, delimiter, values);
            xml->PushTag("row");
            for (size_t i = 0; i < std::max(fieldNames.size(), values.size()); i++) {
                if (i == fieldNames.size()) {
                    // Create a new name using the column number (for both no-header case and excess values case)
                    std::string name = XmlUtils::ToString(i + 1);
                    while (std::find(fieldNames.begin(), fieldNames.end(), name) != fieldNames.end()) {
                        name = "_" + name; // keep prepending _ until we get a unique name
                    }
                    fieldNames.push_back(name); // create more field names
                }
                if (i < values.size()) {
                    std::string& value = values[i];    
                    XmlUtils::Replace(value, "<", "&lt;");
                    XmlUtils::Replace(value, ">", "&gt;");
                    xml->AddData(fieldNames[i], values[i]);
                }
                else {
                    xml->AddData(fieldNames[i], "");
                }
            }
            xml->PopTag();
            m_parser->Parse(strm);
            strm.str(std::string()); // reset
            strm.clear();
        }
        xml->PopTag();
        m_parser->Parse(strm);
        return true;
    }

    std::shared_ptr<StreamingXml::XmlTag> ParseEmbeddedJson(
        std::shared_ptr<StreamingXml::XmlOutput> xml, std::string& msg, bool leaveOuterTagOpen = false)
    {
        std::shared_ptr<StreamingXml::XmlTag> emptyTag;

        int depth = 0;
        const char *beg = nullptr, *end = nullptr;
        for (const char* pos = msg.c_str(); *pos != '\0' && !end; pos++) {
            if (*pos == '{') {
                if (depth++ == 0) {
                    beg = pos;
                }
            }
            else if (*pos == '}') {
                if (depth == 0) {
                    return std::move(emptyTag); // brace out of order
                }
                if (--depth == 0) {
                    end = pos;
                }
            }
        }
        if (!end) {
            return std::move(emptyTag);
        }

        // if there is a label and a colon preceding the object expression, example foo:{a:1}, then include it in the
        // json expression as if it were {foo:{a:1}}
        bool foundColon = false, inLabel = false;
        const char* endLabel = nullptr;
        std::string label;
        const char* pos;
        for (pos = beg - 1; pos >= msg.c_str(); pos--) {
            if (*pos == ':') {
                if (foundColon) { // can't have multiple colons
                    break;
                }
                foundColon = true;
            }
            else if (isalnum(*pos) || (*pos == '_')) {
                if (!inLabel) {
                    if (!foundColon) { // can't start reading label (from end) if we don't have a colon
                        break; // without a label (okay)
                    }
                    inLabel = true;
                    endLabel = pos;
                }
            }
            else if (inLabel) { // finished reading a label, which also implies we found a colon
                break;
            }
            else if (!isspace(*pos)) { // illegal character after possible label
                break;
            }
        }
        // Check to see if the label
        // If we exited the loop being in a label, capture it
        if (inLabel) {
            beg = pos + 1; // adjust the beg
            label = std::string(beg, endLabel - beg + 1);
        }

        size_t len = end - beg + 1;
        std::string str(label.size() 
            ? "{" + std::string(beg, len) + "}"
            : std::string(beg, len)); // reform foo:{a:1} into {foo:{a:1}} if foo exists
        std::stringstream strm(str);
        std::shared_ptr<StreamingXml::XmlTag> outerTag;
        try {
            Json2Xml json2xml(xml, "", leaveOuterTagOpen);
            outerTag = json2xml.Read(strm);
            size_t start = beg - msg.c_str();
            msg.erase(start, len);
        }
        catch (...) {
            // ignore invalid json and false-positively detected json
        }

        return std::move(outerTag);
    }

    void SetInput(bool disallowStdin)
    {
        const std::string& filename = m_inputFilename;
        if (filename.size()) {
            m_input.reset(new std::ifstream(filename));
            if (m_input->fail()) {
                XmlUtils::Error("Input file could not be opened: %s", filename);
            }
        }
        else {
            if (disallowStdin) {
                /**/XmlUtils::Error("Given query requires two passes, so stdin cannot be used as an input.");
            }
            m_input.reset(&std::cin);
        }
    }

    void SetOutput()
    {
        m_output.reset(&std::cout);
        m_xml.reset(new XmlOutput(*m_output));
    }

    void PrintRow(size_t rowIdx)
    {
        if (!m_output) {
            return;
        }

        m_output->setf(std::ios::left);

        if (m_outputHeader) {
            bool first = true;
            for (auto& column : m_parser->GetColumns()) {
                if (!(column->flags & XmlColumn::Output)) {
                    continue;
                }
                if (!first) {
                    *m_output << ",";
                }
                first = false;
                *m_output << XmlUtils::FormatForCsv(column->name);
            }
            *m_output << '\n';
            m_outputHeader = false;
        }

        size_t pivotIndex = -1;
        std::shared_ptr<std::vector<std::string>> pivotPathParts;
        do {
            bool first = true;
            for (auto& column : m_parser->GetColumns()) {
                if (!(column->flags & XmlColumn::Output)) {
                    continue;
                }

                const XmlValue& origValue = m_parser->GetValue(rowIdx, column->valueIdx);

                if (m_values.size() == 0) {
                    m_values.resize(1);
                }
                m_values[0] = std::move(XmlUtils::FormatForCsv(origValue.ToString(XmlValue::SubsecondTimes)));

                for (std::vector<std::string>::iterator it = m_values.begin(); it != m_values.end(); it++) {
                    std::string& value = *it;
                    if (column->expr->GetOperator()->opcode == Opcode::OpPivotPath) {
                        // We encountered a PivotPath expression; get its parts
                        if (pivotIndex == -1) {
                            pivotPathParts.reset(new std::vector<std::string>(std::move(XmlUtils::Split(value, "."))));
                            pivotIndex = 0;
                        }

                        // At each iteration of the pivot, concatenate starting at pivotIndex
                        std::stringstream pivot;
                        for (size_t i = pivotIndex; i < pivotPathParts->size(); i++) {
                            if (i > pivotIndex) {
                                pivot << ".";
                            }
                            pivot << pivotPathParts->at(i);
                        }

                        if (++pivotIndex == pivotPathParts->size()) {
                            pivotIndex = (size_t)-1; // stop iterating
                        }

                        // Reset actual value
                        value = pivot.str();
                    }
                    if (!first) {
                        *m_output << ",";
                    }
                    first = false;
                    *m_output << value;
                }
            }
            *m_output << '\n';
        } while (pivotIndex != -1);
        m_output->flush();
    }

    std::string m_inputFilename;
    bool m_inputHeader;
    bool m_outputHeader;
    std::vector<std::string> m_values; // scratch string vector for output
    std::unique_ptr<std::istream> m_input;
    std::unique_ptr<std::ostream> m_output;
    std::unique_ptr<XmlOutput> m_xml;
    std::unique_ptr<XmlParser> m_parser;
};

} // namespace StreamingXml
