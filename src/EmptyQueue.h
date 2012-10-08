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

#ifndef SRC_EMPTYQUEUE_H_
#define SRC_EMPTYQUEUE_H_
#include <stdint.h>
#include <list>
#include <tr1/unordered_map>

#include "Node.h"

namespace cbt {
    class Node;
    struct NodeID {
        uint32_t operator()(const Node* node) const {
            return node->id();
        }
    };
    struct NodeEqual {
        bool operator()(const Node* lhs, const Node* rhs) const {
            return (lhs->id() == rhs->id());
        }
    };
    typedef std::tr1::unordered_map<Node*, uint32_t, NodeID, NodeEqual> PQ;
    class EmptyQueue {
      public:
        EmptyQueue() {}
        ~EmptyQueue() {}

        void insert(Node* n, uint32_t prio) {
            PQ::iterator it = nodeList_.find(n);
            if (it == nodeList_.end())
                nodeList_[n] = prio;
            else if (it->second < prio)
                it->second = prio;
        }

        Node* pop() {
            PQ::iterator it = nodeList_.begin();
            uint32_t maxPrio = it->second;
            PQ::iterator maxNode = it;
            ++it;
            for (; it != nodeList_.end(); ++it) {
                if (it->second > maxPrio) {
                    maxNode = it;
                    maxPrio = it->second;
                }
            }
            Node* retNode = maxNode->first;
            nodeList_.erase(maxNode);
            return retNode;
        }

        void erase(Node* n) {
        }

        bool empty() const {
            return nodeList_.empty();
        }

        bool contains(Node* n) const {
            PQ::const_iterator it = nodeList_.find(n);
            if (it != nodeList_.end())
                return true;
            return false;
        }

        uint32_t priority(Node* n) const {
            PQ::const_iterator it = nodeList_.find(n);
            return it->second;
        }

        size_t size() const {
            return nodeList_.size();
        }

        void printElements() {
            for (PQ::const_iterator it = nodeList_.begin();
                    it != nodeList_.end(); ++it) {
                if (it->first->isRoot())
                    fprintf(stderr, "%d(%d)*, ", it->first->id(), it->second);
                else
                    fprintf(stderr, "%d(%d), ", it->first->id(), it->second);
            }
            fprintf(stderr, "\n");
        }

      private:
        PQ nodeList_;
    };
}
#endif  // SRC_EMPTYQUEUE_H_
