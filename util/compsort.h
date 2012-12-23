#ifndef COMPSORT_H
#define COMPSORT_H

#define UINT32_MAX  0xffffffff

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <math.h>

using namespace std;

namespace compsort {

    inline uint32_t get_least_sig_k_bitmask(uint32_t k)
    {
        if (k == 32)
            return 0xffffffff;
        return (1 << k) - 1;
    }

    /* Each 32 bit unsigned integer is stored as follows:
     * + the k LSB are stored first
     * + if the number doesn't fit in k bits, the k+1 th bit is 1, else it is 0
     * + the next k LSB are stored next and so on
     */
    bool compress(uint32_t* data, uint32_t len, uint32_t*& out, uint32_t& out_len)
    {
        // k is the number of bits allocated for each value
        uint32_t* last_word = out;
        uint32_t a = 32; // denotes the number of bits remaining in last word
        uint32_t min = UINT32_MAX, max = 0;
        uint32_t k;
        for (int i=len-1; i>=1; i--) {
            data[i] -= data[i-1];
            if (data[i] < min) min = data[i];
            if (data[i] > max) max = data[i];
        }
//        k = floor(log2((max + min) / 2)) + 1;
        k = 16;
        *last_word = k; last_word++; // store k as first element since we need it later
        last_word++; // leave another space for storing offset in the final word
        *last_word = 0;

        for (uint32_t i=0; i<len; ++i) {
            uint32_t n = data[i];
            int32_t m = (n > 0? floor(log2(n)) + 1: 1); // number of bits reqd.
            while (m > 0) {
                uint32_t bm = get_least_sig_k_bitmask(k);
                uint32_t x = n & bm;
                n >>= k;
                m -= k;

                // set overflow bit
                x <<= 1;
                if (m > 0) x |= 1;

                // check if x fits in last_word
                if (k+1 <= a) {
                    x <<= a - (k+1);
                    *last_word |= x;
                    a -= (k+1);
                } else { // the entire fragment doesn't fit in last word
                    // extract a MSB from x
                    uint32_t y = x >> (k+1-a);
                    *last_word |= y;

                    // increment last_word
                    last_word++;
                    *last_word = 0;

                    // get remaining bits
                    y = x & get_least_sig_k_bitmask(k+1-a);
                    y <<= (32 - (k+1-a));
                    *last_word |= y;
                    a = 32 - (k+1-a);
                }
            }
        }
        out_len = (last_word - out) + (a == 32? 0 : 1);
        out[1] = 32 - a; // store final offset in second word
        return true;
    }


    bool decompress(uint32_t* data, uint32_t len, uint32_t*& out, uint32_t& out_len)
    {
        out_len = 0;
        uint32_t k = data[0];
        uint32_t finoff = data[1];
        uint32_t cur = 0;
        uint32_t b = 0; // number of bits in cur so far
        uint32_t rem = 0; // number of bits reqd from next word to make up k+1
        uint32_t x; // stores fragments
        for (uint32_t i=2; i<len; i++) {
            uint32_t n = data[i];
            uint32_t a = 32; // number of bits remaining in current word
            bool of;
            // while we can satisfy the next 'fragment' from the current word
            while (a >= k+1) {
                uint32_t num_bits_to_read;
                if (rem > 0) { // we have a partial fragment from before
                    // read rem bits
                    num_bits_to_read = rem;
                    uint32_t rembm = get_least_sig_k_bitmask(rem);
                    uint32_t y = (n >> (a-rem)) & rembm;
                    x |= y;
                } else { // we're beginning a new fragment
                    // extract next k+1 bits from n into x
                    num_bits_to_read = k + 1;
                    uint32_t bm = get_least_sig_k_bitmask(k + 1);
                    assert(bm > 0);
                    x = (n >> (a-(k+1))) & bm;
                }
                // now x contains an entire (k+1)-length fragment, so we set
                // rem to 0, since we don't need any bits to complete this
                // fragment
                rem = 0;
                of = ((x & 1) == 1);
                x >>= 1; // get rid of overflow bit

                x <<= b;
                cur |= x;
                b += k;
                a -= num_bits_to_read;
                if (!of) {
                    out[out_len++] = cur;
                    cur = 0;
                    b = 0;
                }
                if (i == len-1 && a == 32-finoff)
                    goto exit_loop;
            }
            // if we still have some bits remaining, we start the next fragment
            if (a > 0) {
                // get LS a bits
                uint32_t bm = get_least_sig_k_bitmask(a);
                x = n & bm;
                rem = (k+1-a);
                x <<= rem; // make space for remaining bits
                assert(rem > 0);
            }
        }
exit_loop:
        if (rem != 0) {
            assert(false);
        }
        for (uint32_t i = 1; i < out_len; ++i)
            out[i] += out[i-1];
        return true;
    }
};

#endif  // COMPSORT_H
