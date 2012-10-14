#ifndef SERVICE_CLIENT_H_
#define SERVICE_CLIENT_H_

#include <string>
#include <vector>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "TestApp.h"

namespace cbtservice {
    class CBTClient {
      public:
        CBTClient(uint32_t u, uint32_t l);
        ~CBTClient();
        void Run();

      private:
        // dataset properties
        const uint32_t kNumUniqKeys;
        const uint32_t kKeyLen;
        const uint32_t kNumFillers;
        const uint32_t kLettersInAlphabet;
        const uint32_t kMaxPAOs;

        float Conv26(float x) {
            return x * log(2)/log(26);
        }

        void GenerateFillers(uint32_t filler_len);
        void GeneratePAOs(std::vector<TestPAO*>& paos, uint32_t number_of_paos);
        void SerializePAOs(const std::vector<TestPAO*>& paos,
                google::protobuf::io::CodedOutputStream* cs);
        void DeletePAOs(const std::vector<TestPAO*>& paos);

        TestOperations* to_;

        // dataset generation
        std::vector<std::string> fillers_;
        uint32_t num_full_loops_;
        uint32_t part_loop_; 
    };
} // cbtservice

#endif  // SERVICE_CLIENT_H_
