#pragma once

#include <assert.h>
#include <vector>
#include <algorithm>

#include "fastrandom.hpp"
#include "constants.hpp"

class Rand {
   private:
    bool oc_init = false;
    std::vector<int> oc_id_vector;
    FastRandom r;

   public:
    Rand() : r(0xdeadbeef) {}

    int checkBetweenInclusive(int v, int lower, int upper) {
        assert(v >= lower);
        assert(v <= upper);
        return v;
    }

    int randomNumber(int min, int max) {
        return checkBetweenInclusive((int)(r.next_uniform() * (max - min + 1) + min), min, max);
    }

    int randomNumberExcluding(int min, int max, int exclude)
    {
        int ret = randomNumber(min, max);
        while (ret == exclude) {
            ret = randomNumber(min, max);
        }
        return ret;
    }

    int nonUniformRandom(int A, int C, int min, int max) {
        return (((randomNumber(0, A) | randomNumber(min, max)) + C) % (max - min + 1)) + min;
    }

    int generate_random_o_c_id() {
        if (!oc_init) {
            oc_init = true;
            for (int i = 0; i < 3000; i++) oc_id_vector.push_back(i);
            std::random_shuffle(oc_id_vector.begin(), oc_id_vector.end());
        }
        int ret = oc_id_vector.back();
        oc_id_vector.pop_back();
        return ret;
    }

    std::vector<uint32_t> make_permutation(int min, int max){

        std::vector<uint32_t> ret;
        for (int i = min; i <= max; i++) ret.push_back(i);
        std::random_shuffle(ret.begin(), ret.end());
        return ret;
    }

    uint32_t GetCurrentTime() {
        static __thread uint32_t tl_hack = 100000;
        return ++tl_hack;
    }

    std::string generateRandomString(int length) {
        std::string s(length, 0);
        for (int i = 0; i < length; i++) {
            s[i] = r.next_readable_char();
        }
        return s;
    }

    std::string randomNStr(uint len) {
        const char base = '0';
        std::string buf(len, 0);
        for (uint i = 0; i < len; i++) buf[i] = (char)(base + (r.next() % 10));
        return buf;
    }

    std::string generateRandomString(int min_chars, int max_chars) {
        int len = randomNumber(min_chars, max_chars);
        return generateRandomString(len);
    }

    void Lastname(int num, std::string& name) {
        std::string n[10] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
        name = n[num / 100] + n[(num / 10) % 10] + n[num % 10];
    }

    std::string getCustomerLastName(int num) {
        std::string buf;
        Lastname(num, buf);
        return std::string(buf);
    }

    std::string getNonUniformCustomerLastNameLoad() { return getCustomerLastName(nonUniformRandom(255, 157, 0, 999)); }

    //   return CheckBetweenInclusive(NonUniformRandom(1023, 259, 1, g_tpcc_config.customers_per_district),
    //                            1, g_tpcc_config.customers_per_district);
    int transactionGetRandomCustomerID() {
        return checkBetweenInclusive(nonUniformRandom(1023, 259, 1, CUSTOMERS_PER_DISTRICT), 1, CUSTOMERS_PER_DISTRICT);
    }
};
