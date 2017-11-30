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

#include <fstream>
#include <iostream>
#include <istream>
#include <string>

namespace StreamingXml
{

class XmlCheck
{
public:
    static bool IsXmlFile(const std::string& filename, std::istream** pinput = 0, char* ungetChar = 0)
    {
        std::istream* input = &std::cin;
        bool isStdin = true;
        if (!filename.empty()) {
            isStdin = false;
            input = new std::ifstream(filename.c_str());
            if (!input->good()) {
                delete input;
                return false;
            }
        }

        char ch = ' ';
        while (isspace(ch) && input->good() && !input->eof()) {
            input->read(&ch, 1);
        }
        if (isspace(ch)) {
            delete input;
            return false;
        }

        if (!isStdin) {
            input->seekg(0);
        }
        else if (ungetChar != 0) {
            *ungetChar = ch;
        }

        if (pinput != 0) {
            *pinput = input;
        }
        else if (!isStdin) {
            delete input;
        }

        return (ch == '<'); // are we looking at an XML file?
    }
};

} // namespace StreamingXml
