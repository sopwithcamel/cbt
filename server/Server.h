
#ifndef SERVICE_SERVER_H_
#define SERVICE_SERVER_H_

#include "CompressTree.h"
#include "TestApp.h"

namespace cbtservice {
    class CBTServer {
      public:
        static CBTServer* Instance();
        void Start();
        void Stop();

      private:
        const uint32_t kPAOsInsertAtTime;

        void Run();
        CBTServer();
        ~CBTServer();
        bool HandleMessage(const std::string& message,
                uint32_t& num_PAOs);

        static CBTServer* instance_;
        cbt::CompressTree* cbt_;
        TestOperations* to_;
        PartialAgg** recv_paos_;
    };
} // cbtservice

#endif  // SERVICE_SERVER_H_
