#ifndef SERVICE_ZIPF_H
#define SERVICE_ZIPF_H
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>   
#include <math.h>

#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>
#include <iostream>

namespace cbtservice {
    class ZipfGenerator {
      public:
        ZipfGenerator(double alpha, uint32_t n) :
                alpha_(alpha),
                max_(n) {
            // Compute normalization constant
            for (uint32_t i = 1; i <= max_; ++i)
                c = c + (1.0 / pow((double) i, alpha_));
            c = 1.0 / c;

            sum_prob_ = new double[max_ + 1];
            sum_prob_[0] = 0;
            for (uint32_t i = 1; i <= n; ++i)
                sum_prob_[i] = sum_prob_[i - 1] + c / pow((double) i, alpha_);
        }

        ~ZipfGenerator() {
            delete[] sum_prob_;
        }

        uint32_t operator()() {
            double z;                     // Uniform random number (0 < z < 1)
            double zipf_value = max_;            // Computed exponential value to be returned

            // Pull a uniform random number (0 < z < 1)
            do {
                z = dist_(gen_);
            } while (z == 0 || z == 1);

            // Map z to the value
            for (uint32_t i = 1; i <= max_; ++i) {
                if (sum_prob_[i] >= z) {
                    zipf_value = i;
                    break;
                }
            }

            // Assert that zipf_value is between 1 and N
            if ((zipf_value < 1) || (zipf_value > max_)) {
                std::cout << zipf_value << std::endl;
                assert(false);
            }
            return (zipf_value);
        }

      private:
        double alpha_;
        uint32_t max_;
        double c;          // Normalization constant
        double* sum_prob_;              // Sum of probabilities

        boost::mt19937 gen_;
        boost::uniform_01<> dist_;
    };
}  // cbtservice

#endif  // SERVICE_ZIPF_H
