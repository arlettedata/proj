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
#include <math.h>
#include <set>

#include "xmlpath.h"

namespace StreamingXml
{

class XmlMatcher
{
public:
    typedef XmlRowMatchState::MatchType MatchType;

    XmlMatcher(XmlParserContextPtr context, XmlPaths& paths)
        : m_context(context)
        , m_paths(paths)
        , m_rowState(new XmlRowMatchState)
    {
        Initialize();
        Reset();

        // Mark all the paths to which syncTag refers. This is called a rollback at this point.
        for (auto p = m_paths.begin(); p != m_paths.end(); p++) {
            XmlPath& path = **p;
            XmlPath::Tag* tag = &path.m_tagList.front();
            if (tag->m_wildcard) {
                tag = tag->next(); // skip wildcard
            }
        }
    }

    bool MatchStartTag(const char* tag, size_t len)
    {
        bool matchDetected = false;

        m_rowState->matchType = XmlRowMatchState::NotAllMatched;

        if (m_paths.size()) {
            if (len == (size_t)-1) {
                len = strlen(tag);
            }

            m_rowState->currParseDepth++;
            for (auto& path : m_paths) {
                matchDetected |= path->Path_MatchStartTag(tag, len);
            }

            if (matchDetected) {
                for (auto& path : m_paths) {
                    // reset any "sequentially later" matches to keep things in sync
                    path->Path_Reset(m_rowState->currParseDepth, m_rowState->matchOrder);
                }
            }
        }

        m_context->appendingValues |= (m_rowState->searchingForEndTagCnt > 0);

        return matchDetected;
    }

    bool MatchEndTag(const char* tag, size_t len)
    {
        bool matchDetected = false;
        if (m_paths.size()) {
            for (auto& path : m_paths) {
                matchDetected |= path->Path_MatchEndTag(tag, len);
            }
            m_rowState->currParseDepth--;
        }
        m_context->appendingValues |= (m_rowState->searchingForEndTagCnt > 0);
        return matchDetected;
    }

    void CommitMatch()
    {
        for (auto& path : m_paths) {
            path->RemoveValueIndents();
            path->StartMatch();
        }
    }

    MatchType GetMatchType()
    {
        bool allMatched = (m_paths.size() > 0);
        bool withNoDataMatches = false;
        for (auto& path : m_paths) {
            if (path->m_flags & XmlPath::Sync && path->IsMatched()) {
                allMatched = true;
                break; // no other criteria needed if we matched on a sync path
            }
            if ((path->m_flags & XmlPath::NoData && path->m_pathRef->endMatchExprs.size() == 0) &&
                (path->m_matchState == XmlPath::SearchingForEndTag)) {
                // relaxed matching: we don't need to reach the end tag when the path doesn't need data. e.g. attribute
                // lookup
                allMatched = true;
                //withNoDataMatches = true; // TODO: fixed an edge. Do we need this anymore?
            }
            else if (path->IsMatched()) {
                allMatched = true;
            }
            else {
                allMatched = false;
                withNoDataMatches = false;
            }
            if (!allMatched) {
                break;
            }
        }

        m_rowState->matchType = !allMatched
            ? MatchType::NotAllMatched
            : (withNoDataMatches ? MatchType::AllMatchedWithNoDataMatches : MatchType::AllMatched);
        return m_rowState->matchType;
    }

    void Initialize()
    {
        for (auto& path : m_paths) {
            path->m_rowState = m_rowState;
        }
    }

    void Rollback()
    {
        m_rowState->matchType = MatchType::NotAllMatched;
        m_rowState->matchOrder = 0;
        for (auto& path : m_paths) {
            path->Path_Rollback(m_rowState->currParseDepth, -1);
        }
    }

    void Reset()
    {
        m_rowDomain.clear();
        m_rowState->Reset();
        for (auto& path : m_paths) {
            path->Path_Reset(-1, -1);
        }

        // XmlPaths always start with a wildcard, p->g. *.foo, where * means "match 1 or more"
        // So we "wrap" the XML state with an outer tag.  This also lets us stream multiple XML
        // files as if it were one.
        static const std::string root = "__root";
        MatchStartTag(root.c_str(), root.length());
    }

    XmlParserContextPtr m_context;
    XmlPaths& m_paths;
    XmlRowMatchStatePtr m_rowState;
    std::set<std::string> m_rowDomain;
};

} // namespace StreamingXml
