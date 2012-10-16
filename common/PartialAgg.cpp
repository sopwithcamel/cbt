// Copyright (C) 2012 Georgia Institute of Technology
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

// ---
// Author: Hrishikesh Amur

#include <stdint.h>
#include "PartialAgg.h"

Token::Token() {
}

void Token::init(const Token& rhs) {
    uint32_t i;
    void* buf;
    for (i=0; i<rhs.tokens.size(); i++) {
        buf = malloc(rhs.token_sizes[i] + 1);
        memcpy(buf, rhs.tokens[i], rhs.token_sizes[i]);
        tokens.push_back(buf);
        token_sizes.push_back(rhs.token_sizes[i]);
    }
}

Token::~Token() {
}

void Token::clear()
{
	tokens.clear();
	token_sizes.clear();
}

PartialAgg::PartialAgg() {
}

PartialAgg::~PartialAgg() {
}

Operations::Operations() {
}

Operations::~Operations() {
}
