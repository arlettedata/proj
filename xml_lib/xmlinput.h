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

#include "xmlutils.h"
#include <assert.h>
#include <fstream>
#include <iostream>
#include <memory.h>
#include <string>

namespace StreamingXml
{

class XmlInput : public std::string
{
public:
    XmlInput()
    {
        m_fd = stdin;
        m_nextInputBuf[0] = '\0';
        m_firstRead = true;
        m_lookupDirs = 0;
        m_inputStream = 0;
    }

    ~XmlInput()
    {
        if ((m_fd != 0) && (m_fd != stdin)) {
            fclose(m_fd);
        }
    }

    void UngetString(const std::string& ungetString)
    {
        assert(ungetString.length() <= BUFSIZE);
        strcpy(m_nextInputBuf, ungetString.c_str());
    }

    void SetLookupDirectories(const std::vector<std::string>& _lookupDirs)
    {
        m_lookupDirs = &_lookupDirs;
    }

    bool ReadFromFile(const std::string& filename)
    {
        if (m_fd == stdin) {
            size_t lookupDirNum = 0;
            std::string lookupDir;
            while (true) {
                std::string filePath = lookupDir + filename;
                m_fd = fopen(filePath.c_str(), "r");
                if ((m_fd != 0) || (m_lookupDirs == 0) || (lookupDirNum == m_lookupDirs->size())) {
                    break;
                }
                lookupDir = (*m_lookupDirs)[lookupDirNum++];
                XmlUtils::AppendSlash(lookupDir, true);
            }

            if (m_fd == 0) {
                throw std::exception();
            }
        }

        bool moreInput = _Read();

        if (!moreInput) {
            fclose(m_fd);
            m_fd = 0;
        }

        return moreInput;
    }

    bool ReadFromFile(const std::string& filename, const std::string& terminators, bool balanceParens = false)
    {
        if ((m_fd == 0) || (m_fd == stdin)) {
            const char* szName = filename.c_str();
            m_fd = fopen(szName, "r");
            if (m_fd == 0) {
                throw std::exception();
            }
        }

        bool moreInput = _Read(&terminators, balanceParens);

        if (!moreInput) {
            if (m_fd != stdin) {
                fclose(m_fd);
                m_fd = 0;
            }
        }

        return moreInput;
    }

    bool ReadFromConsole()
    {
        m_fd = stdin;
        return _Read();
    }

    bool ReadFromConsole(const std::string& terminators, bool balanceParens = false)
    {
        m_fd = stdin;
        return _Read(&terminators, balanceParens);
    }

    bool ReadFromStream(std::istream& input)
    {
        m_fd = 0;
        m_inputStream = &input;
        return _Read();
    }

    bool ReadFromStream(std::istream& input, const std::string& terminators, bool balanceParens = false)
    {
        m_fd = 0;
        m_inputStream = &input;
        return _Read(&terminators, balanceParens);
    }

private:
    enum
    {
        BUFSIZE = 128
    };

    bool _Read(const std::string* terminators = 0, bool balanceParens = false)
    {
        bool moreInput = false;
        const int bufsize = BUFSIZE;
        int numOpenParens = 0, numCloseParens = 0;
        const char* terminatorChars = (terminators == 0) ? "" : terminators->c_str();

        clear();

        while (!EndOfFile() || (m_nextInputBuf[0] != '\0')) {
            size_t bytes;
            char buf[BUFSIZE + 1];

            if (m_nextInputBuf[0] != '\0') {
                // we have data from the last call to _Read, use it first
                strcpy(buf, m_nextInputBuf);
                m_nextInputBuf[0] = '\0';
            }
            else {
                bytes = ReadChars(buf, bufsize); // truncation deliberate
                buf[bytes] = '\0';
                if (m_firstRead) {
                    char* pos = buf;

                    // skip past initial terminators and whitespace
                    for (; *pos != '\0'; pos++) {
                        if (!isspace((unsigned char)*pos) && (strchr(terminatorChars, *pos) == 0)) {
                            break;
                        }
                    }

                    if (pos != buf) {
                        memmove(buf, pos, sizeof(buf) - (int)(pos - buf));
                    }

                    m_firstRead = false;
                }
            }

            //
            // If terminators are specified, then more input is possible.  Check for them to see if
            // current input is complete.  Also handle paren balancing.
            //
            if (terminators != 0) {
                char* termPos = buf;
                bool acceptTerminator = true;
                if (balanceParens) {
                    char* pos = buf;
                    while (*pos != '\0') {
                        if ((strchr(terminatorChars, *pos) != 0) && (numOpenParens == numCloseParens)) {
                            termPos = pos;
                            break;
                        }
                        if (*pos == '(') {
                            numOpenParens++;
                        }
                        if (*pos == ')') {
                            numCloseParens++;
                        }
                        pos++;
                    }
                    //
                    // Note: the calling object isn't providing information whether there needs to be
                    // at least 0 or 1 set of parens.  The problem with 0 is that in some cases where
                    // there are multiple terminators between a set of input characters can cause an
                    // "empty" input to be returned (the parser stops between terminators rather than
                    // keep reading paren-containing input.).
                    //
                    acceptTerminator = ((numOpenParens != 0) && (numOpenParens == numCloseParens));
                }
                if (acceptTerminator) {
                    char* pos = (char*)_find_first_of(termPos, terminatorChars);
                    if (pos != 0) {
                        // walk past all terminators (we might have consecutive ones, in which case we ignore)
                        bool foundterm;
                        do {
                            foundterm = (*pos != '\0') && (strchr(terminatorChars, *pos) != 0);
                            if (foundterm) {
                                *pos++ = '\0';
                            }
                        } while (foundterm);
                        strcpy(m_nextInputBuf, pos);
                        *pos = '\0';

                        //
                        // do a quick check to see if the semicolon is at the end of the file, in which case we don't
                        // care about it this is a bit tricky to know given the limited buffer size.  If something other
                        // than whitespace, then we have more input, otherwise we're done despite the terminator
                        //
                        size_t len = strlen(m_nextInputBuf);

                        moreInput = false;
                        for (pos = m_nextInputBuf; !moreInput && (*pos != '\0'); pos++) {
                            if (!isspace((unsigned char)*pos) && (strchr(terminatorChars, *pos) == 0)) {
                                moreInput = true;
                                break;
                            }
                        }

                        pos = m_nextInputBuf + len;
                        while (!moreInput && !EndOfFile() && (len < BUFSIZE)) { // peek ahead one character at a time
                            if (ReadChars(pos, 1) == 1) {
                                // don't count spaces or additional terminators as part of next input
                                moreInput = !isspace((unsigned char)*pos) && (strchr(terminatorChars, *pos) == 0);
                                if (moreInput) {
                                    // we want to keep this character for the next input
                                    pos++;
                                    len++;
                                }
                                *pos = '\0';
                            }
                        }
                    }
                }
            }

            *this += buf;

            //
            // If moreInput was flipped on, then we saw a terminator and divided the buffer read from the file
            // into the return buffer and m_nextInputBuf, so we can exit now.
            //
            if (moreInput) {
                break;
            }
        }

        return moreInput;
    }

    bool EndOfFile()
    {
        if (m_inputStream != 0) {
            return m_inputStream->eof();
        }
        else {
            return (feof(m_fd) != 0);
        }
    }

    size_t ReadChars(char* buf, size_t bufsize)
    {
        if (m_inputStream != 0) {
            m_inputStream->read(buf, (std::streamsize)bufsize);
            return (size_t)m_inputStream->gcount();
        }
        else {
            return fread((void*)buf, sizeof(char), bufsize, m_fd);
        }
    }

    static const char* _find_first_of(const char* buf, const char* terms)
    {
        for (const char* pos = buf; *pos != '\0'; pos++) {
            for (const char* term = terms; *term != '\0'; term++) {
                if (*pos == *term) {
                    return pos;
                }
            }
        }
        return 0;
    }

    const std::vector<std::string>* m_lookupDirs;
    char m_nextInputBuf[BUFSIZE + 1];
    bool m_firstRead;
    FILE* m_fd;
    std::istream* m_inputStream;
};

} // namespace StreamingXml
