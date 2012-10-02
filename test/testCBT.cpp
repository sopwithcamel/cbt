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
            TestOperations* to = new TestOperations();
            cbt_ = new cbt::CompressTree(2, fanout, 1000, buffer_size, pao_size,
                    to);
        }

        // Code in the TearDown() method will be called immediately after each test
        // (right before the destructor).
        virtual void TearDown() {
            delete cbt_;
        }

        // Objects declared here can be used by all tests
        CompressTree* cbt_;

    };
}  // namespace cbt

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
