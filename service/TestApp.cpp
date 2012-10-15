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

#include "TestApp.h"

using namespace google::protobuf::io;

namespace cbtservice {
    TestPAO::TestPAO(const std::string& key, uint32_t val) {
        pb.set_key(key);
        pb.set_count(val);
    }

    TestPAO::~TestPAO() {
    }

    const char* TestOperations::getKey(PartialAgg* p) const {
        TestPAO* wp = (TestPAO*)p;
        return wp->pb.key().c_str();
    }

    bool TestOperations::setKey(PartialAgg* p, char* k) const
    {
        TestPAO* wp = (TestPAO*)p;
        wp->pb.set_key(k);
        return true;
    }

    uint32_t TestOperations::getValue(PartialAgg* p) const {
        TestPAO* wp = (TestPAO*)p;
        return wp->pb.count();
    }

    bool TestOperations::setValue(PartialAgg* p, uint32_t newval) const {
        TestPAO* wp = (TestPAO*)p;
        wp->pb.set_count(newval);
        return true;
    }

    bool TestOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const {
        TestPAO* wp1 = (TestPAO*)p1;
        TestPAO* wp2 = (TestPAO*)p2;
        return (!wp1->pb.key().compare(wp2->pb.key()));
    }

    size_t TestOperations::createPAO(Token* t, PartialAgg** p) const {
        if (t == NULL) {
            p[0] = new TestPAO("", 0);
        } else {
            assert(false && "this isn't handled :(");
        }
        return 1;
    }

    bool TestOperations::destroyPAO(PartialAgg* p) const {
        TestPAO* wp = (TestPAO*)p;
        delete wp;
        return true;
    }

    bool TestOperations::merge(PartialAgg* p, PartialAgg* mg) const {
        TestPAO* wp = (TestPAO*)p;
        TestPAO* wmp = (TestPAO*)mg;
        wp->pb.set_count(wp->pb.count() + wmp->pb.count());
        return true;
    }

    uint32_t TestOperations::getSerializedSize(PartialAgg* p) const {
        TestPAO* wp = (TestPAO*)p;
        return wp->pb.ByteSize();
    }

    bool TestOperations::serialize(PartialAgg* p,
            char* output, size_t size) const {
        TestPAO* wp = (TestPAO*)p;
        memset((void*)output, 0, size);
        return wp->pb.SerializeToArray(output, size);
    }

    bool TestOperations::deserialize(PartialAgg* p,
            const char* input, size_t size) const {
        TestPAO* wp = (TestPAO*)p;
        return wp->pb.ParseFromArray(input, size);
    }

    bool TestOperations::serialize(PartialAgg* p,
            google::protobuf::io::CodedOutputStream* output) const {
        TestPAO* t = (TestPAO*)p;
        output->WriteVarint32(t->pb.ByteSize());
        t->pb.SerializeToCodedStream(output);
        return true;
    }

    bool TestOperations::deserialize(PartialAgg* p,
            google::protobuf::io::CodedInputStream* input) const {
        uint32_t bytes;
        TestPAO* t = (TestPAO*)p;
        input->ReadVarint32(&bytes);
        CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
        bool ret = t->pb.ParseFromCodedStream(input);
        input->PopLimit(msgLimit);
        return ret;
    }
}  // cbtservice
