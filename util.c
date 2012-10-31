#include <stdint.h>

uint64_t ntohll(uint64_t a) {
        uint32_t lo = a & 0xffffffff;
        uint32_t hi = a >> 32U;
        lo = ntohl(lo);
        hi = ntohl(hi);
        return ((uint64_t) lo) << 32U | hi;
}
