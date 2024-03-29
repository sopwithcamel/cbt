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

#ifndef SERVICE_CLIENT_H_
#define SERVICE_CLIENT_H_

#include <string>
#include <vector>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "PartialAgg.h"

namespace cbtservice {

    typedef enum {
        POWERLAW,
        UNIFORM,
    } InputDistribution;

    class CBTClient {
      public:
        CBTClient(uint32_t u, uint32_t l, InputDistribution d);
        ~CBTClient();
        void Run();

      private:
        // dataset properties
        const uint32_t kNumUniqKeys;
        const uint32_t kKeyLen;
        const InputDistribution kInputDist;
        const uint32_t kNumFillers;
        const uint32_t kLettersInAlphabet;
        const uint32_t kMaxPAOs;

        float Conv26(float x) {
            return x * log(2)/log(26);
        }

        void GenerateFillers(uint32_t filler_len);
        void SetupGenerators();
        void GeneratePAOs(std::vector<PartialAgg*>& paos,
                uint32_t number_of_paos);
        void GenerateUniformPAOs(std::vector<PartialAgg*>& paos,
                uint32_t number_of_paos);
        void GeneratePowerLawPAOs(std::vector<PartialAgg*>& paos,
                uint32_t number_of_paos);
        bool LinkUserMap();
        void SerializePAOs(const std::vector<PartialAgg*>& paos,
                google::protobuf::io::CodedOutputStream* cs);
        void DeletePAOs(const std::vector<PartialAgg*>& paos);

        Operations* to_;

        // dataset generation
        std::vector<char*> fillers_;
    };
} // cbtservice

#endif  // SERVICE_CLIENT_H_
