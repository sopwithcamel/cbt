#include "PartialAgg.h"
#include "test.pb.h"

using namespace std;

class TestOperations : public Operations
{
  public:
    // gettors and settors
    const char* getKey(PartialAgg* p) const;
    bool setKey(PartialAgg* p, char* k) const;
    uint32_t getValue(PartialAgg* p) const;
    bool setValue(PartialAgg* p, uint32_t newval) const;

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const;
    bool destroyPAO(PartialAgg* p) const;
    bool merge(PartialAgg* p, PartialAgg* mg) const;
    inline uint32_t getSerializedSize(PartialAgg* p) const;
    inline bool serialize(PartialAgg* p, char* output, size_t size) const;
    inline bool deserialize(PartialAgg* p, const char* input,
            size_t size) const;

  private:
    SerializationMethod getSerializationMethod() const {
        return PROTOBUF;
    }
    size_t createPAO(Token* t, PartialAgg** p) const {
        return 0;
    }
    size_t dividePAO(const PartialAgg& p, PartialAgg** p_list) const {
        return 0;
    }
    inline bool serialize(PartialAgg* p, std::string* output) const {
        return false;
    }
    inline bool deserialize(PartialAgg* p, const std::string& input) const {
        return false;
    }
    inline bool serialize(PartialAgg* p,
            google::protobuf::io::CodedOutputStream* output) const {
        return false;
    }
    inline bool deserialize(PartialAgg* p,
            google::protobuf::io::CodedInputStream* input) const {
        return false;
    }
};

class TestPAO : public PartialAgg {
  public:
    TestPAO(const std::string& key, uint32_t val);
    ~TestPAO();

  private:
    friend class TestOperations;
    test::pao pb;
};

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

bool TestOperations::setValue(PartialAgg* p, uint32_t newval) const
{
    TestPAO* wp = (TestPAO*)p;
    wp->pb.set_count(newval);
    return true;
}

bool TestOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const
{
    TestPAO* wp1 = (TestPAO*)p1;
    TestPAO* wp2 = (TestPAO*)p2;
    return (!wp1->pb.key().compare(wp2->pb.key()));
}

bool TestOperations::destroyPAO(PartialAgg* p) const
{
    TestPAO* wp = (TestPAO*)p;
    delete wp;
    return true;
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
        char* output, size_t size) const
{
    TestPAO* wp = (TestPAO*)p;
    memset((void*)output, 0, size);
    return wp->pb.SerializeToArray(output, size);
}

inline bool TestOperations::deserialize(PartialAgg* p,
        const char* input, size_t size) const
{
    TestPAO* wp = (TestPAO*)p;
    return wp->pb.ParseFromArray(input, size);
}
