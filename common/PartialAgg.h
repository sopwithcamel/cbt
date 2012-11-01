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

#ifndef SRC_PARTIALAGG_H_
#define SRC_PARTIALAGG_H_
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sstream>
#include <vector>

#define REGISTER(x)\
        extern "C" Operations* __libminni_create_ops()\
        {\
            return new x();\
        }


class Token {
  public:
	Token() {}
	~Token() {}
	void init(const Token& rhs) {
        uint32_t i;
        void* buf;
        for (i=0; i<rhs.tokens.size(); i++) {
            buf = malloc(rhs.token_sizes[i] + 1);
            memcpy(buf, rhs.tokens[i], rhs.token_sizes[i]);
            tokens.push_back(buf);
            token_sizes.push_back(rhs.token_sizes[i]);
        }
    }
	void clear() {
        tokens.clear();
        token_sizes.clear();
    }
	std::vector<void*> tokens;
	std::vector<size_t> token_sizes;
};

class PartialAgg {
  protected:
    /* don't allow PartialAgg objects to be created */
    PartialAgg() {}
    ~PartialAgg() {}
};

class Operations {
  public:
    static uint64_t createCtr;
    static uint64_t destCtr;

    enum SerializationMethod {
        PROTOBUF,
        BOOST,
        HAND
    };
    Operations() {}
    virtual ~Operations() {}
    virtual const char* getKey(PartialAgg* p) const = 0;
    virtual bool setKey(PartialAgg* p, char* k) const = 0;
    virtual void* getValue(PartialAgg* p) const = 0;
    virtual void setValue(PartialAgg* p, void* v) const = 0;
    virtual bool sameKey(PartialAgg* p1, PartialAgg* p2) const = 0;
    virtual size_t createPAO(Token* t, PartialAgg** p_list) const = 0;
    virtual size_t dividePAO(const PartialAgg& p,
            PartialAgg** p_list) const = 0;
    virtual bool destroyPAO(PartialAgg* p) const = 0;
    virtual bool merge(PartialAgg* v, PartialAgg* merge) const = 0;
    virtual SerializationMethod getSerializationMethod() const = 0;
    virtual uint32_t getSerializedSize(PartialAgg* p) const = 0;
    /* serialize into string/buffer */
    virtual bool serialize(PartialAgg* p,
            std::string* output) const = 0;
    virtual bool serialize(PartialAgg* p,
            char* output, size_t size) const = 0;
    /* deserialize from string/buffer */
    virtual bool deserialize(PartialAgg* p,
            const std::string& input) const = 0;
    virtual bool deserialize(PartialAgg* p,
            const char* input, size_t size) const = 0;
};

#endif  // SRC_PARTIALAGG_H_
