#include "CompressTree.h"
#include "test.pb.h"

using namespace google::protobuf::io;
using namespace std;

class TestOperations : public Operations
{
  public:
    const char* getKey(PartialAgg* p) const;
    bool setKey(PartialAgg* p, char* k) const;
    bool sameKey(PartialAgg* p1, PartialAgg* p2) const;
    size_t createPAO(Token* t, PartialAgg** p) const;
    size_t dividePAO(const PartialAgg& p, PartialAgg** p_list) const;
    bool destroyPAO(PartialAgg* p) const;
    bool merge(PartialAgg* p, PartialAgg* mg) const;
    SerializationMethod getSerializationMethod() const {
        return PROTOBUF;
    }
    inline uint32_t getSerializedSize(PartialAgg* p) const;
    inline bool serialize(PartialAgg* p, std::string* output) const;
    inline bool serialize(PartialAgg* p, char* output, size_t size) const;
    inline bool deserialize(PartialAgg* p, const std::string& input) const;
    inline bool deserialize(PartialAgg* p, const char* input,
            size_t size) const;
    inline bool serialize(PartialAgg* p,
            google::protobuf::io::CodedOutputStream* output) const;
    inline bool deserialize(PartialAgg* p,
            google::protobuf::io::CodedInputStream* input) const;
};

class TestPAO : public PartialAgg {
  public:
    TestPAO();
    ~TestPAO();

  private:
    friend class TestOperations;
    test::pao pb;
};

TestPAO::TestPAO()
{
}

TestPAO::~TestPAO()
{
}

const char* TestOperations::getKey(PartialAgg* p) const
{
    TestPAO* wp = (TestPAO*)p;
    return wp->pb.key().c_str();
}

bool TestOperations::setKey(PartialAgg* p, char* k) const
{
    TestPAO* wp = (TestPAO*)p;
    wp->pb.set_key(k);
}

bool TestOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const
{
    TestPAO* wp1 = (TestPAO*)p1;
    TestPAO* wp2 = (TestPAO*)p2;
    return (!wp1->pb.key().compare(wp2->pb.key()));
}


size_t TestOperations::createPAO(Token* t, PartialAgg** p) const
{
    if (t == NULL) {
        p[0] = new TestPAO();
    } else {
        TestPAO* wp = (TestPAO*)(p[0]);
        wp->pb.set_key((char*)t->tokens[0]);
        wp->pb.set_count(1);
    }
    return 1;
}

size_t TestOperations::dividePAO(const PartialAgg& p,
        PartialAgg** p_list) const
{
    return 1;
}

bool TestOperations::destroyPAO(PartialAgg* p) const
{
    TestPAO* wp = (TestPAO*)p;
    delete wp;
}


bool TestOperations::merge(PartialAgg* p, PartialAgg* mg) const
{
    TestPAO* wp = (TestPAO*)p;
    TestPAO* wmp = (TestPAO*)mg;
    wp->pb.set_count(wp->pb.count() + wmp->pb.count());
    return true;
}

inline uint32_t TestOperations::getSerializedSize(PartialAgg* p) const
{
    TestPAO* wp = (TestPAO*)p;
    return wp->pb.ByteSize();
}

bool TestOperations::serialize(PartialAgg* p,
        string* output) const
{
    TestPAO* wp = (TestPAO*)p;
    wp->pb.SerializeToString(output);
}

bool TestOperations::serialize(PartialAgg* p,
        char* output, size_t size) const
{
    TestPAO* wp = (TestPAO*)p;
    memset((void*)output, 0, size);
    wp->pb.SerializeToArray(output, size);
}

inline bool TestOperations::deserialize(PartialAgg* p,
        const string& input) const
{
    TestPAO* wp = (TestPAO*)p;
    return wp->pb.ParseFromString(input);
}

inline bool TestOperations::deserialize(PartialAgg* p,
        const char* input, size_t size) const
{
    TestPAO* wp = (TestPAO*)p;
    return wp->pb.ParseFromArray(input, size);
}

bool TestOperations::serialize(PartialAgg* p,
        CodedOutputStream* output) const
{
    TestPAO* wp = (TestPAO*)p;
    output->WriteVarint32(wp->pb.ByteSize());
    wp->pb.SerializeToCodedStream(output);
}

bool TestOperations::deserialize(PartialAgg* p,
        CodedInputStream* input) const
{
    uint32_t bytes;
    TestPAO* wp = (TestPAO*)p;
    input->ReadVarint32(&bytes);
    CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
    bool ret = wp->pb.ParseFromCodedStream(input);
    input->PopLimit(msgLimit);
    return ret;
}

REGISTER(TestOperations);
