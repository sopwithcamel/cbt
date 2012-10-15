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

#ifndef SERVICE_TESTAPP_H_
#define SERVICE_TESTAPP_H_

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "PartialAgg.h"
#include "PAO.pb.h"

namespace cbtservice {

    class TestPAO;

    class TestOperations : public Operations {
      public:
        // gettors and settors
        const char* getKey(PartialAgg* p) const;
        bool setKey(PartialAgg* p, char* k) const;
        uint32_t getValue(PartialAgg* p) const;
        bool setValue(PartialAgg* p, uint32_t newval) const;

        bool sameKey(PartialAgg* p1, PartialAgg* p2) const;
        size_t createPAO(Token* t, PartialAgg** p) const;
        bool destroyPAO(PartialAgg* p) const;
        bool merge(PartialAgg* p, PartialAgg* mg) const;
        uint32_t getSerializedSize(PartialAgg* p) const;
        bool serialize(PartialAgg* p, char* output, size_t size) const;
        bool deserialize(PartialAgg* p, const char* input,
                size_t size) const;
        bool serialize(PartialAgg* p, std::string* output) const {
            return false;
        }
        bool deserialize(PartialAgg* p, const std::string& input) const {
            return false;
        }
        bool serialize(PartialAgg* p,
                google::protobuf::io::CodedOutputStream* output) const;
        bool deserialize(PartialAgg* p,
                google::protobuf::io::CodedInputStream* input) const;

      private:
        SerializationMethod getSerializationMethod() const {
            return PROTOBUF;
        }
        size_t dividePAO(const PartialAgg& p, PartialAgg** p_list) const {
            return 0;
        }
    };

    class TestPAO : public PartialAgg {
      public:
        TestPAO(const std::string& key, uint32_t val);
        ~TestPAO();

        test::PAO pb;

      private:
        friend class TestOperations;
    };
} // cbtservice

#endif  // SERVICE_TESTAPP_H_
