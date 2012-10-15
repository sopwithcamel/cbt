
#ifndef SERVICE_SERVER_H_
#define SERVICE_SERVER_H_
#include <pthread.h>
#include <stdint.h>

#include "CompressTree.h"
#include "TestApp.h"

namespace cbtservice {
    class CBTServer {
      public:
        static CBTServer* Instance();
        void Start();
        void Stop();
        static void* CallHelper(void*);
      private:
        const uint32_t kPAOsInsertAtTime;

        CBTServer();
        ~CBTServer();
        bool HandleMessage(const std::string& message,
                uint32_t& num_PAOs);
        void Run();
        void Timer();

        static CBTServer* instance_;
        cbt::CompressTree* cbt_;
        TestOperations* to_;
        PartialAgg** recv_paos_;

        uint64_t total_PAOs_inserted_;
    };
} // cbtservice

#endif  // SERVICE_SERVER_H_
