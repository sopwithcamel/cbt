#include <gtest/gtest.h>
#include "CompressTree.h"
#include "TestApp.h"

namespace cbt {
    class CBTTest : public testing::Test {
      protected:
        // Code here will be called immediately after the constructor (right before
        // each test).
        virtual void SetUp() {
            uint32_t fanout = 8;
            uint32_t buffer_size = 31457280;
            uint32_t pao_size = 20;
            to = new TestOperations();
            cbt = new cbt::CompressTree(2, fanout, 1000, buffer_size, pao_size,
                    to);
        }

        // Code in the TearDown() method will be called immediately after each test
        // (right before the destructor).
        virtual void TearDown() {
            delete cbt;
            delete to;
        }

        // Objects declared here can be used by all tests
        CompressTree* cbt;
        TestOperations* to;

    };

    void test_insert(CompressTree* c) {
        TestPAO* t = new TestPAO("test", 1);
        ASSERT_TRUE(c->insert(static_cast<PartialAgg*>(t)));
        delete t;
    }

    void test_aggregate(CompressTree* c) {
        TestPAO* t = new TestPAO("test", 1);
        for (uint32_t i = 0; i < 10; ++i) {
            ASSERT_TRUE(c->insert(static_cast<PartialAgg*>(t)));
        }
        TestPAO* t2 = new TestPAO("", 0);
        PartialAgg* p = static_cast<PartialAgg*>(t2);
        int hash;
        void* ptr = reinterpret_cast<void*>(&hash);
        ASSERT_TRUE(c->nextValue(ptr, p));
        delete t;
        delete t2;            
    }

    TEST_F(CBTTest, TestInsert) {
        test_insert(cbt);
    }

    TEST_F(CBTTest, TestAggregate) {
        test_aggregate(cbt);
    }
}  // namespace cbt

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
