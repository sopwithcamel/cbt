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

#include <assert.h>
#include <dlfcn.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

#include <gflags/gflags.h>
#include <gperftools/heap-profiler.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <zmq.hpp>
#include "zmq_helpers.hpp"
#include <string>
#include <iostream>

#include "CompressTree.h"
#include "PartialAgg.h"
#include "ProtobufPartialAgg.h"
#include "Server.h"

using namespace google::protobuf::io;
using namespace std;

DEFINE_bool(timed, false, "Do a timed run");
DEFINE_bool(heapcheck, false, "Heap check");

namespace cbtservice {
    // Global static pointer used to ensure a single instance of the class.
    CBTServer* CBTServer::instance_ = NULL; 

    // This function is called to create an instance of the class.  Calling the
    // constructor publicly is not allowed. The constructor is private and is
    // only called by this Instance function.
    CBTServer* CBTServer::Instance() {
        if (!instance_) {   // Only allow one instance of class to be generated.
            instance_ = new CBTServer();
        }
        return instance_;
    }

    void CBTServer::Start() {
        Instance()->Run();
    }

    void CBTServer::Stop() {
        stop_server_ = true;
        fprintf(stderr, "Stopping Server...\n");

        // flushing tree
        int hash; void* ptr = reinterpret_cast<void*>(&hash);
        PartialAgg* test;
        to_->createPAO(NULL, &test);
        cbt_->nextValue(ptr, test);
        sleep(2);
        cbt_->clear();
        delete Instance();
    }

    CBTServer::CBTServer() :
            kPAOsInsertAtTime(100000),
            kMaxUniquePAOs(10000000),
            stop_server_(false),
            total_PAOs_inserted_(0) {
        uint32_t fanout = 8;
        uint32_t buffer_size = 31457280;
        uint32_t pao_size = 20;

        // First create an Operations object
        assert(LinkUserMap());

        cbt_ = new cbt::CompressTree(2, fanout, 1000, buffer_size, pao_size,
                to_);
        fprintf(stderr, "CBTServer created\n");

        recv_paos_ = reinterpret_cast<PartialAgg**>(malloc(sizeof(PartialAgg*)
                * kPAOsInsertAtTime));
        for (uint32_t i = 0; i < kPAOsInsertAtTime; ++i) {
            to_->createPAO(NULL, &recv_paos_[i]);
        }
        send_paos_ = reinterpret_cast<PartialAgg**>(malloc(sizeof(PartialAgg*)
                * kPAOsInsertAtTime));
    }

    CBTServer::~CBTServer() {
        for (uint32_t i = 0; i < kPAOsInsertAtTime; ++i)
            to_->destroyPAO(recv_paos_[i]);
        free(recv_paos_);
        free(send_paos_);
        delete cbt_;
        delete to_;
    }

    void CBTServer::Run() {
        //  Prepare our context and socket
        zmq::context_t context (1);
        zmq::socket_t socket (context, ZMQ_REP);
        socket.bind ("tcp://*:5555");

        while (!stop_server_) {
            bool ret;
            uint32_t num_received_PAOs;

            //  Wait for next request from client
            string msg = s_recv(socket);

            // Deserialize PAOs and insert into CBT
            ret = HandleMessage(msg, num_received_PAOs);
    
            if (ret) {
                total_PAOs_inserted_ += num_received_PAOs;
//                cout << "Inserted " << total_PAOs_inserted_ << endl;
            } else {
                printf("ERROR\n");
            }
                
            //  Send reply back to client
            //  1. set up a coded stream to wrap a stringstream
            std::stringstream ss;
            OstreamOutputStream* os = new OstreamOutputStream(&ss);
            CodedOutputStream* cs = new CodedOutputStream(os);

            //  2. read the results from the CBT
            uint64_t num_read;
            bool remain = cbt_->bulk_read(send_paos_, num_read,
                    kMaxUniquePAOs);
            if (remain)
                fprintf(stderr, "More results remaining\n");

            //  3. serialize results to the stream
            for (uint32_t i = 0; i < num_read; ++i) {
                assert(static_cast<ProtobufOperations*>(to_)->serialize(
                            static_cast<PartialAgg*>(send_paos_[i]), cs));
            }

            //  4. delete the wrapper streams to flush results to stringstream
            delete cs;
            delete os;

            //  5. set up zeromq message to send reply
            s_send(socket, ss.str());
            fprintf(stderr, "Sent %d results to client\n", num_read);

            //  6. clean up
            for (uint32_t i = 0; i < num_read; ++i)
                static_cast<ProtobufOperations*>(to_)->destroyPAO(
                        static_cast<PartialAgg*>(send_paos_[i]));
                
        }
    }

    bool CBTServer::HandleMessage(const string& message,
            uint32_t& num_PAOs) {
        stringstream ss;
        ss << message;
        IstreamInputStream* ii = new IstreamInputStream(&ss);
        CodedInputStream* ci = new CodedInputStream(ii);

        uint32_t rem = message.size();
        num_PAOs = 0;
        do {
            uint32_t to_insert= 0;
            for ( ; to_insert < kPAOsInsertAtTime; ++to_insert) {
                if (rem == 0)
                    break;
                assert(static_cast<ProtobufOperations*>(to_)->deserialize(
                        recv_paos_[to_insert], ci));
                rem -= (to_->getSerializedSize(recv_paos_[to_insert]) + 1);
            }
            assert(cbt_->bulk_insert(recv_paos_, to_insert));
            num_PAOs += to_insert;
        } while (rem > 0);

//        cout << "Inserted " << num_PAOs << " PAOs" << endl;

        delete ci;
        delete ii;
        return true;
    }

    bool CBTServer::LinkUserMap() { 
        const char* err;
        void* handle;
        std::string soname = "/usr/local/lib/minni/wc_proto.so";
        handle = dlopen(soname.c_str(), RTLD_LAZY);
        if (!handle) {
            fputs(dlerror(), stderr);
            return false;
        }

        Operations* (*create_ops_obj)() = (Operations* (*)())dlsym(handle,
                "__libminni_create_ops");
        if ((err = dlerror()) != NULL) {
            fprintf(stderr, "Error locating symbol __libminni_create_ops\
                    in %s\n", err);
            exit(-1);
        }
        to_ = create_ops_obj();
        return true;
    }

    void CBTServer::Timer() {
        uint64_t last_PAOs_inserted = 0;
        while (!stop_server_) {
            cout << (total_PAOs_inserted_ - last_PAOs_inserted) << endl;
            last_PAOs_inserted = total_PAOs_inserted_;
            sleep(1);
        }
    }

    void* CBTServer::CallHelper(void*) {
        Instance()->Timer();   
        pthread_exit(NULL);
    }
} // cbtservice

void INThandler(int sig) {
    cbtservice::CBTServer::Instance()->Stop();   
    exit(0);
}

int main (int argc, char** argv) {
    // Define gflags options
    google::ParseCommandLineFlags(&argc, &argv, true);

    // Check if we are doing a timed run
    if (FLAGS_timed) {
        pthread_t timer_thread_;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        int rc = pthread_create(&timer_thread_, &attr,
                cbtservice::CBTServer::CallHelper, NULL);
        if (rc) {
            cerr << "ERROR; return code from pthread_create() is "
                    << rc << endl;
            exit(-1);
        }
        sleep(1);
    }

    signal(SIGINT, INThandler);

/*
    if (FLAGS_heapcheck)
        HeapProfilerStart("/tmp/cbtserver");
*/

    cbtservice::CBTServer::Instance()->Start();   

/*
    if (FLAGS_heapcheck)
        HeapProfilerStop();
*/
    return 0;
}
