/**
 * author : gusimiu@baidu.com
 * version : 
 *      1.0 complete the code.
 *
 * date : 2014-09-20
 * brief : 
 *  KeyIndex_t is for fast random search for key->value.
 *  template input:
 *      KeyInfo_t {
 *          size_t key;
 *          # other info you wanted.
 *      };
 *  
 *  usage:
 *      KeyIndex_t<YourType> index;
 *      for .. {
 *          index.append_key_info(keyinfo);
 *      }
 *      index.make_index();
 *      index.write(output_file);
 *      index.read(input_file);
 *      if (index.seek(key, &out)) {
 *          // do something.
 *      }
 *
 */

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <sys/time.h>

#include <algorithm>
#include "kvindex.h"

class Timer {
public:
    Timer() :_sum(0.0) {}

    void begin() {
        gettimeofday(&_begin_tv, NULL);
    }

    void end() {
        gettimeofday(&_end_tv, NULL);
        _sum = (_end_tv.tv_sec - _begin_tv.tv_sec) + (_end_tv.tv_usec - _begin_tv.tv_usec) * 0.000001f;
    }

    /* return unit : seconds.*/
    float cost_time() const {
        return _sum;
    }

private:
    float   _sum;

    timeval _begin_tv;
    timeval _end_tv;
};

void 
test_keyindex_benchmark(
        size_t data_count, size_t query_count) {
    Timer timer;
    timer.begin();
    srand(time(NULL));
    u64_t *key_list = new u64_t[data_count];
    KeyIndex_t<KVD_KeyInfo_t> keyind(data_count);
    for (size_t i=0; i<data_count; ++i) {
        u64_t x = rand();
        x = (x<<32LL) + rand();
        key_list[i] = x;
        KVD_KeyInfo_t ki;
        // fake data.
        ki.key = x;
        ki.data_offset = i;
        keyind.append_key_info(ki);
    }
    timer.end();
    fprintf(stderr, "generate random data ok. [%.3f]\n", timer.cost_time());
    timer.begin();
    keyind.make_index();
    timer.end();
    fprintf(stderr, "make_index ok [%.3f].\n", timer.cost_time());
    timer.begin();
    int ok = 0;
    for (size_t i=0; i<query_count; ++i) {
        KVD_KeyInfo_t out;
        if (keyind.find(key_list[i] + ((int)i % 3)-1, &out)) {
            ok += 1;
        }
    }
    timer.end();
    fprintf(stderr, "seek ok. ok:%d [%.3fs] qps=%.1f\n", 
            ok, timer.cost_time(), query_count/timer.cost_time());
}

void 
test_keyvalue_benchmark(
        size_t data_count, size_t query_count) {
    Timer timer;
    timer.begin();
    srand(time(NULL));
    u64_t *key_list = new u64_t[data_count];
    KVMemoryBatchDict_t key_value_dict;
    for (size_t i=0; i<data_count; ++i) {
        u64_t x = rand();
        x = (x<<32LL) + rand();
        key_list[i] = x;

        char value[32];
        snprintf(value, 32, "%llu", x);

        key_value_dict.append(x, value, strlen(value)+1);
    }
    timer.end();
    fprintf(stderr, "generate random data ok. [%.3f]\n", timer.cost_time());

    timer.begin();
    key_value_dict.append_complete();
    timer.end();
    fprintf(stderr, "append_complete_over ok [%.3f].\n", timer.cost_time());

    timer.begin();
    key_value_dict.write("test_kv.dict");
    timer.end();
    fprintf(stderr, "write to dict ok [%.3f].\n", timer.cost_time());

    timer.begin();
    KVMemoryBatchDict_t new_kvdict;
    new_kvdict.read("test_kv.dict");
    timer.end();
    fprintf(stderr, "read to dict ok [%.3f].\n", timer.cost_time());

    timer.begin();
    int ok = 0;
    for (size_t i=0; i<query_count; ++i) {
        char out[32];
        size_t s = 32;
        if (new_kvdict.find(key_list[i], out, &s)) {
            ok += 1;
        }
    }
    timer.end();
    fprintf(stderr, "seek ok. ok:%d [%.3fs] qps=%.1f\n", 
            ok, timer.cost_time(), query_count/timer.cost_time());
}

int main(int argc, const char** argv) {
    test_keyindex_benchmark(10000000, 1000000);
    //test_keyvalue_benchmark(50000000, 1000000);
    return 0;
}
