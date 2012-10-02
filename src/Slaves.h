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
      public:
        Slave(CompressTree* tree);
        virtual ~Slave() {}
        virtual void addNode(Node* node) = 0;
        virtual bool empty() const;
        virtual bool wakeup();    
        virtual void waitUntilCompletionNoticeReceived();

        void startThreads();
        void stopThreads();
      protected:
        static void* callHelper(void* context);
        virtual void printElements() const;
        virtual void sendCompletionNotice();
        virtual void setInputComplete(bool value) { inputComplete_ = value; }
        virtual void* work() = 0;

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
      private:
        friend class CompressTree;
        friend class Node;
    };

    class Emptier : public Slave {
        struct PrioComp {
            bool operator()(uint32_t lhs, uint32_t rhs)
            {
                return (lhs > rhs);
            }
        };
      public:
        Emptier(CompressTree* tree);
        ~Emptier();
        void* work();
        void addNode(Node* node);
      private:
        friend class CompressTree;
        friend class Node;

        EmptyQueue queue_;
    };

    class Compressor : public Slave {
      public:
        Compressor(CompressTree* tree);
        ~Compressor();
        void* work();
        void addNode(Node* node);
      private:
        friend class CompressTree;
        friend class Node;
    };

    class Sorter : public Slave {
      public:
        Sorter(CompressTree* tree);
        ~Sorter();
        void* work();
        void addNode(Node* node);
      private:
        friend class CompressTree;
        friend class Node;
    };

    class Pager : public Slave {
      public:
        Pager(CompressTree* tree);
        ~Pager();
        void* work();
        void addNode(Node* node);
      private:
        friend class CompressTree;
        friend class Node;
    };

#ifdef ENABLE_COUNTERS
    class Monitor : public Slave {
      public:
        static void* callHelper(void* context);
        Monitor(CompressTree* tree);
        ~Monitor();
        void* work();
        void addNode(Node* n);
      private:
        friend class CompressTree;
        friend class Node;
        friend class Compressor;

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
