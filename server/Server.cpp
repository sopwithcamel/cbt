#include <assert.h>
#include <signal.h>
#include <unistd.h>

#include <zmq.hpp>
#include "zhelpers.hpp"
#include <string>
#include <iostream>

#include "CompressTree.h"
#include "Server.h"
#include "TestApp.h"

using namespace google::protobuf::io;

namespace cbtservice {
    // Global static pointer used to ensure a single instance of the class.
    CBTServer* CBTServer::instance_ = NULL; 

    // This function is called to create an instance of the class.  Calling the
    // constructor publicly is not allowed. The constructor is private and is
    // only called by this Instance function.
    CBTServer* CBTServer::Instance() {
        if (!instance_)   // Only allow one instance of class to be generated.
            instance_ = new CBTServer();
        return instance_;
    }

    void CBTServer::Start() {
        instance_->Run();
    }

    void CBTServer::Stop() {
        delete instance_;
    }

    CBTServer::CBTServer() :
            kPAOsInsertAtTime(100000) {
        uint32_t fanout = 8;
        uint32_t buffer_size = 31457280;
        uint32_t pao_size = 20;
        to_ = new TestOperations();
        cbt_ = new cbt::CompressTree(2, fanout, 1000, buffer_size, pao_size,
                to_);

        recv_paos_ = reinterpret_cast<PartialAgg**>(malloc(sizeof(PartialAgg*)
                * kPAOsInsertAtTime));
        for (uint32_t i = 0; i < kPAOsInsertAtTime; ++i)
            recv_paos_[i] = new TestPAO("", 0);
    }

    CBTServer::~CBTServer() {
        for (uint32_t i = 0; i < kPAOsInsertAtTime; ++i)
            to_->destroyPAO(recv_paos_[i]);
        free(recv_paos_);
        delete cbt_;
        delete to_;
    }

    void CBTServer::Run() {
        //  Prepare our context and socket
        zmq::context_t context (1);
        zmq::socket_t socket (context, ZMQ_REP);
        socket.bind ("tcp://*:5555");

        while (true) {
            bool ret;
            uint32_t num_received_PAOs;

            //  Wait for next request from client
            std::string msg = s_recv(socket);

            // Deserialize PAOs and insert into CBT
            ret = HandleMessage(msg, num_received_PAOs);
    
            //  Send reply back to client
            zmq::message_t reply (5);
            memcpy((void *)reply.data(), ret? "True" : "False", 5);
            socket.send(reply);
        }
    }

    bool CBTServer::HandleMessage(const std::string& message,
            uint32_t& num_PAOs) {
        std::stringstream ss;
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
                assert(to_->deserialize(recv_paos_[to_insert], ci));
                rem -= (to_->getSerializedSize(recv_paos_[to_insert]) + 1);
            }
            assert(cbt_->bulk_insert(recv_paos_, to_insert));
            std::cout << "Inserted " << to_insert << " PAOs" << std::endl;
            num_PAOs += to_insert;
        } while (rem > 0);

        delete ci;
        delete ii;
        return true;
    }
} // cbtservice

void INThandler(int sig) {
    cbtservice::CBTServer::Instance()->Stop();   
    exit(0);
}

int main () {
    signal(SIGINT, INThandler);
    cbtservice::CBTServer::Instance()->Start();   
    return 0;
}
