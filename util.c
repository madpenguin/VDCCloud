#include <stdint.h>
#include <string.h>

uint64_t ntohll(uint64_t a) {
        uint32_t lo = a & 0xffffffff;
        uint32_t hi = a >> 32U;
        lo = ntohl(lo);
        hi = ntohl(hi);
        return ((uint64_t) lo) << 32U | hi;
}

const char *byte_to_binary(int x)
{
    static char b[9];
    b[0] = '\0';

    int z;
    for (z = 128; z > 0; z >>= 1)
    {
        strcat(b, ((x & z) == z) ? "1" : "0");
    }

    return b;
}
