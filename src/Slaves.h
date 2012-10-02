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

#ifndef SRC_SLAVES_H_
#define SRC_SLAVES_H_
#include <stdint.h>
#include <deque>
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
        explicit Slave(CompressTree* tree);
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
            bool operator()(uint32_t lhs, uint32_t rhs) {
                return (lhs > rhs);
            }
        };
      public:
        explicit Emptier(CompressTree* tree);
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
        explicit Compressor(CompressTree* tree);
        ~Compressor();
        void* work();
        void addNode(Node* node);

      private:
        friend class CompressTree;
        friend class Node;
    };

    class Sorter : public Slave {
      public:
        explicit Sorter(CompressTree* tree);
        ~Sorter();
        void* work();
        void addNode(Node* node);

      private:
        friend class CompressTree;
        friend class Node;
    };

    class Pager : public Slave {
      public:
        explicit Pager(CompressTree* tree);
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
        explicit Monitor(CompressTree* tree);
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
#endif  // SRC_SLAVES_H_
