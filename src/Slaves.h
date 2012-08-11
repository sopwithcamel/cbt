#ifndef LIB_COMPRESS_SLAVES_H
#define LIB_COMPRESS_SLAVES_H
#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>

#include "CompressTree.h"
#include "Node.h"
#include "EmptyQueue.h"

namespace cbt {
    class CompressTree;
    class Node;

    class Slave {
        friend class CompressTree;
        friend class Node;
      public:
        Slave(CompressTree* tree);
        virtual ~Slave() {}
        virtual void* work() = 0;
        virtual void addNode(Node* node) = 0;
        virtual bool empty() const;
        virtual bool wakeup();    
        virtual void setInputComplete(bool value) { inputComplete_ = value; }
        virtual void sendCompletionNotice();
        virtual void waitUntilCompletionNoticeReceived();

        virtual void printElements() const;
      protected:
        CompressTree* tree_;
        bool inputComplete_;
        bool queueEmpty_;

        pthread_t thread_;
        pthread_cond_t queueHasWork_;
        pthread_mutex_t queueMutex_;

        pthread_mutex_t completionMutex_;
        pthread_cond_t complete_;
        bool askForCompletionNotice_;

        std::deque<Node*> nodes_;
    };

    class Emptier : public Slave {
        friend class CompressTree;
        friend class Node;
        struct PrioComp {
            bool operator()(uint32_t lhs, uint32_t rhs)
            {
                return (lhs > rhs);
            }
        };
      public:
        static void* callHelper(void* context);
        Emptier(CompressTree* tree);
        ~Emptier();
        void* work();
        void addNode(Node* node);
      private:
        EmptyQueue queue_;
    };

    class Compressor : public Slave {
        friend class CompressTree;
        friend class Node;
      public:
        static void* callHelper(void* context);
        Compressor(CompressTree* tree);
        ~Compressor();
        void* work();
        void addNode(Node* node);
    };

    class Sorter : public Slave {
        friend class CompressTree;
        friend class Node;
      public:
        static void* callHelper(void* context);
        Sorter(CompressTree* tree);
        ~Sorter();
        void* work();
        void addNode(Node* node);
    };

    class Pager : public Slave {
        friend class CompressTree;
        friend class Node;
      public:
        static void* callHelper(void* context);
        Pager(CompressTree* tree);
        ~Pager();
        void* work();
        void addNode(Node* node);
    };

#ifdef ENABLE_COUNTERS
    class Monitor : public Slave {
        friend class CompressTree;
        friend class Node;
        friend class Compressor;
      public:
        static void* callHelper(void* context);
        Monitor(CompressTree* tree);
        ~Monitor();
        void* work();
        void addNode(Node* n);
      private:
        uint64_t numElements;
        uint64_t numMerged;
        std::vector<uint64_t> elctr;
        std::vector<int32_t> nodeCtr;
        std::vector<int32_t> totNodeCtr;
        uint64_t actr;
        uint64_t bctr;
        uint64_t cctr;
    };
#endif
}

#endif
