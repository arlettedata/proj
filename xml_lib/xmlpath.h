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
#include <float.h>
#include <iomanip>
#include <list>
#include <math.h>

#include "xmlexpr.h"

namespace StreamingXml
{

struct XmlRowMatchState
{
    XmlRowMatchState()
    {
        Reset();
    }

    void Reset()
    {
        matchOrder = 0;
        currParseDepth = 0;
        searchingForEndTagCnt = 0;
        matchType = NotAllMatched;
    }

    int matchOrder;
    int currParseDepth;
    int searchingForEndTagCnt;

    enum MatchType
    {
        NotAllMatched,
        AllMatched,
        AllMatchedWithNoDataMatches
    } matchType;
};

typedef std::shared_ptr<XmlRowMatchState> XmlRowMatchStatePtr;

class XmlPath
{
private:
    class Tag
    {
    public:
        Tag(const std::string& name, bool wildCard, bool first, bool last)
            : m_name(name)
            , m_wildcard(wildCard)
            , m_first(first)
            , m_last(last)
            , m_relativeParseDepth(0)
        {
        }

        int GetRelativeParseDepth(bool includeInitialWildcard) const
        {
            if (m_last) {
                return 1;
            }
            bool initialWildcard = m_first && m_wildcard;
            bool skip = initialWildcard && !includeInitialWildcard;
            return (skip ? 0 : m_relativeParseDepth) + next()->GetRelativeParseDepth(false);
        }

        bool TagList_MatchStartTag(const char* tag, size_t len, int currParseDepth, bool& completeMatch)
        {
            // Before matching the input tag, advance to the next unmatched position in the taglist
            // Advancing a given tag position means that its m_relativeParseDepth is greater than 0.
            if (m_relativeParseDepth > 0) {
                if (next()) {
                    // Check if the next position gives us a match, in which case we'd advance to it.
                    if (next()->TagList_MatchStartTag(tag, len, currParseDepth - m_relativeParseDepth, completeMatch)) {
                        return true;
                    }
                    if (m_wildcard && (next()->m_relativeParseDepth == 0)) {
                        // Stay at this wildcard
                        if (currParseDepth > 0) {
                            m_relativeParseDepth++;
                        }
                        return true;
                    }
                }
            }
            else {
                // Handle 0+ matches for wildcard: if we are at a wildcard
                // and the next position of the taglist matches, then advance position.
                if (m_wildcard && next()) {
                    const char* str = next()->m_name.c_str();
                    if( XmlUtils::stringsEqCase(str, tag, len) && (str[len] == '\0')) {
                        // Advance past the wildcard by recognizing the match at this position.
                        if (currParseDepth > 0) {
                            m_relativeParseDepth++;
                        }
                        // Take care of actual tag match a recursive call.
                        return next()->TagList_MatchStartTag(tag, len, currParseDepth - m_relativeParseDepth, completeMatch);
                    }
                }
                // Handle 1+ matches for wildcard, or regular tag match at current position of taglist
                const char* str = m_name.c_str();
                if (m_wildcard || (XmlUtils::stringsEqCase(str, tag, len) && (str[len] == '\0'))) {
                    if (currParseDepth > 0) {
                        m_relativeParseDepth++;
                    }
                    if (m_last) {
                        completeMatch = true;
                    }
                    return true;
                }
            }
            return false;
        }

        bool TagList_MatchEndTag(const char* tag, size_t len)
        {
            if (next() && (next()->m_relativeParseDepth > 0)) {
                return next()->TagList_MatchEndTag(tag, len);
            }
            const char* str = m_name.c_str();
            if ((m_relativeParseDepth > 0) &&
                (m_wildcard || (XmlUtils::stringsEqCase(str, tag, len) && (str[len] == '\0')))) {
                m_relativeParseDepth--;
                return true;
            }
            return false;
        }

        void Tag_Reset(int rollbackDepth)
        {
            if (rollbackDepth == -1) {
                // rollback started at previous location, so zero out the rest
                m_relativeParseDepth = 0;
                if (next()) {
                    next()->Tag_Reset(-1); // clear out the rest
                }
            }
            else if (rollbackDepth < m_relativeParseDepth) {
                // rollback at current location
                m_relativeParseDepth = rollbackDepth;
                if (next()) {
                    next()->Tag_Reset(-1); // clear out the rest
                }
            }
            else if (next()) {
                // don't rollback at the current location, so decrement rollback value (for relativeness) and keep
                // looking
                next()->Tag_Reset(rollbackDepth - m_relativeParseDepth);
            }
        }

        void Tag_Rollback(int setParseDepth)
        {
            m_relativeParseDepth = setParseDepth;
            if (next()) {
                next()->Tag_Reset(-1); // clear out the rest
            }
        }

        const Tag* next() const
        {
            return m_last ? nullptr : this + 1;
        }

        Tag* next()
        {
            return m_last ? nullptr : this + 1;
        }

    private:
        std::string m_name;
        bool m_wildcard;
        bool m_first;
        bool m_last;
        int m_relativeParseDepth;

        friend class XmlPath;
        friend class XmlMatcher;
    };

public:
    enum MatchState
    {
        Uninitialized,
        SearchingForStartTag,
        CompletingStartTag,
        SearchingForEndTag,
        FoundEndTag
    };

    enum Flags
    {
        ExistsInInput = 0x1,
        NoData = 0x2,
        Sync = 0x4
    };

    XmlPath(XmlParserContextPtr context, XmlPathRefPtr pathRef)
        : m_context(context)
        , m_flags(0)
        , m_pathRef(pathRef)
        , m_str(pathRef->parsedValue.sval)
        , m_matchOrder(-1)
        , m_localMatchDepth(-1)
        , m_matchDepth(-1)
        , m_mismatchDepth(0)
        , m_matchState(Uninitialized)
    {
        // Insert wildcard prefix if there isn't already one, and create tag list
        std::vector<std::string> tags(std::move(XmlUtils::Split(m_pathRef->pathSpec, ".", "{}")));
        if (tags.front() != "*") {
            tags.insert(tags.begin(), "*");
        }
        for (size_t i = 0; i < tags.size(); i++) {
            std::string& tag = tags[i];
            bool wildcard = tag == "*";
            if (tag.front() == '{') {
                assert(tag.back() == '}');
                tag = tag.substr(1, tag.size() - 2);
            }
            m_tagList.push_back(std::move(Tag(tag, wildcard, i == 0, i == tags.size() - 1)));
        }
        if (m_pathRef->flags & XmlPathRef::NoData && !(m_pathRef->flags & XmlPathRef::Data)) {
            m_flags |= XmlPath::NoData;
        }
        if (m_pathRef->flags & XmlPathRef::Sync) {
            m_flags |= XmlPath::Sync;
        }
    }

    // Required by std::vector. We don't expect to use it.
    XmlPath& operator=(const XmlPath& p)
    {
        assert(false);
        return *this;
    }

    operator std::string()
    {
        return std::string("Path(") + m_pathRef->pathSpec + ")";
    }

    bool Path_MatchStartTag(const char* tag, size_t len)
    {
        bool matched = false;
        bool completeMatch = false;

        if ((m_matchState != FoundEndTag) && (m_matchState != SearchingForEndTag)) {
            if (m_mismatchDepth > 0) {
                m_mismatchDepth++;
            }
            else if (!m_tagList.begin()->TagList_MatchStartTag( tag, len, m_rowState->currParseDepth, completeMatch)) {
                m_mismatchDepth++;
            }
            else if (!completeMatch) {
                m_matchState = CompletingStartTag;
            }
            else {
                // Handle a matched start tag

                // Maintain a match order among the fields; this is part of what it means to
                // discover a relationship among the fields.  Optional fields do not participate when
                // restricted matching rules are in effect.
                if (m_matchOrder == -1) {
                    m_matchOrder = m_rowState->matchOrder++;
                }
                else if (m_matchOrder < m_rowState->matchOrder) {
                    m_rowState->matchOrder = m_matchOrder + 1;
                }

                m_str.clear(); // start appending to value now until we find the end tag
                m_matchState = SearchingForEndTag;
                m_matchDepth = m_rowState->currParseDepth;
                m_rowState->searchingForEndTagCnt++;
                m_context->relativeDepth = m_tagList.begin()->GetRelativeParseDepth(false);
                // Reevaluate expressions that directly depend on the start match
                XmlExprEvaluator evaluator(m_context);
                for (auto& expr : m_pathRef->startMatchExprs) {
                    evaluator.ImmedEvaluate(expr);
                }
                m_flags |= ExistsInInput;
                m_pathRef->flags |= XmlPathRef::Matched;
                matched = true;
            }
        }

        return matched;
    }

    bool Path_MatchEndTag(const char* tag, size_t len)
    {
        bool matched = false;

        if (m_mismatchDepth > 0) {
            m_mismatchDepth--;
        }
        else if (m_tagList.begin()->TagList_MatchEndTag(tag, len)) {
            // Handle a matched end tag
            if (m_matchState == SearchingForEndTag) {
                XmlUtils::TrimWhitespace(m_str);
                assert(m_rowState->searchingForEndTagCnt > 0);
                m_rowState->searchingForEndTagCnt--;
                m_matchState = FoundEndTag;
                // Remember the depth where the path was matched
                m_context->relativeDepth = m_tagList.begin()->GetRelativeParseDepth(false);
                m_localMatchDepth = m_rowState->currParseDepth - m_context->relativeDepth;
                // Reevaluate expressions that directly depend on the end match
                XmlExprEvaluator evaluator(m_context);
                for (auto& expr : m_pathRef->endMatchExprs) {
                    evaluator.ImmedEvaluate(expr);
                }
                matched = true;
            }
        }

        return matched;
    }

    bool IsMatched() const
    {
        if ((m_matchState == SearchingForEndTag) || (!(m_pathRef->flags & XmlPathRef::Matched) && m_str.empty())) {
            return false;
        }
        return true;
    }

    void Path_Rollback(int setParseDepth, int matchOrder)
    {
        if (m_matchOrder >= matchOrder) {
            ClearValues(true);
            m_mismatchDepth = 0;
            StartMatch();
            m_tagList.begin()->Tag_Rollback(setParseDepth);
        }
    }

    void Path_Reset(int parseDepth, int matchOrderStart)
    {
        if (m_matchOrder >= matchOrderStart) {
            ClearValues(true);
        }
        m_tagList.begin()->Tag_Reset(parseDepth);
    }

    void ClearValues(bool hardClear)
    {
        m_pathRef->flags &= ~XmlPathRef::Matched;
        m_localMatchDepth = -1;
        m_matchDepth = -1;
        m_str.clear();
        if (hardClear) {
            m_matchOrder = -1;
            StartMatch();
        }
    }

    void StartMatch()
    {
        m_matchState = SearchingForStartTag;
    }

    void SetValue(const std::string& value)
    {
        m_str = value;
    }

    void AppendValue(const char* value, size_t len)
    {
        if (!(m_flags & NoData) && (m_matchState == SearchingForEndTag) && len) {
            m_str.append(value, len);
        }
    }

    void CheckUnreferenced() const
    {
        if (!(m_flags & ExistsInInput)) {
            XmlUtils::Error("Path not matched in %sinput: %s",
                (m_pathRef->flags & XmlPathRef::Joined) ? "joined " : "",
                m_pathRef->pathSpec);
        }
    }

    void RemoveValueIndents()
    {
        //
        // If Value is an XML tree, it may look like <tag>...\n(first-order indent)...\n(second-order indent)... etc
        // find the first-order indent and then subtract out that length in all subsequent indents
        //
        char *valueBeg = (char*)m_str.c_str(), *pos = valueBeg;
        if ((*pos == '<') && (pos = strchr(pos, '\n')) != 0) {
            int indentLength;
            for (indentLength = 0; isspace((unsigned char)*++pos); indentLength++)
                ;
            indentLength -= 2;
            if (indentLength > 0) {
                std::stringstream in(valueBeg);
                std::stringstream out;
                std::string line;
                bool firstLine = true;
                while (XmlUtils::GetLine(in, line)) {
                    if (!firstLine) {
                        pos = &line[0];
                        for (int n = 0; isspace((unsigned char)*++pos) && (n < indentLength); n++)
                            ;
                        out << pos << std::endl;
                    }
                    firstLine = false;
                }
                m_str = out.str();
            }
        }
    }


private:
    friend class XmlMatcher;

    unsigned int m_flags;
    XmlParserContextPtr m_context;
    XmlPathRefPtr m_pathRef;
    std::string& m_str; // binds to m_pathRef->parsedValue.sval
    std::vector<Tag> m_tagList;
    XmlRowMatchStatePtr m_rowState;
    MatchState m_matchState;
    int m_matchOrder;
    int m_localMatchDepth;
    int m_matchDepth;
    int m_mismatchDepth;
};

typedef std::shared_ptr<XmlPath> XmlPathPtr;
typedef std::vector<XmlPathPtr> XmlPaths;

} // namespace StreamingXml
