#ifndef RLE_H
#define RLE_H

#include <stdint.h>
#include <stdlib.h>
#include <math.h>
#include <assert.h>

namespace rle {
    /* Simple run-length encoding on uint32_t values */
    bool encode(uint32_t* data, uint32_t len, uint32_t*& out, uint32_t& out_len)
    {
        out_len = 0;
        uint32_t ctr = 1;
        for (uint32_t i=1; i<len; i++) {
            if (data[i] == data[i-1])
                ctr++;
            else {
                out[out_len++] = data[i-1];
                out[out_len++] = ctr;
                ctr = 1;
            }        
        }
        out[out_len++] = data[len-1];
        out[out_len++] = ctr;
        return true;
    }


    bool decode(uint32_t* data, uint32_t len, uint32_t*& out, uint32_t& out_len)
    {
        out_len = 0;
        for (uint32_t i=0; i<len; i+=2) {
            for (uint32_t j=0; j<data[i+1]; j++)
                out[out_len++] = data[i];
        }
        return true;
    }
};

#endif  // RLE_H
