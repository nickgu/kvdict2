/**
 * author : gusimiu@baidu.com
 * version : 
 *      1.0     complete the code.
 *      1.1     add Buffer_t and KVMemoryBatchDict_t
 *
 * date : 2014-09-22
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
 *      if (index.find(key, &out)) {
 *          // do something.
 *      }
 *
 */

#ifndef __KVINDEX_H__
#define __KVINDEX_H__

#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <algorithm>

/*
 * if not use ratio search.
 * use binary search instead.
 */
//#define USE_RATIO_SEARCH

typedef unsigned long long u64_t;

struct KVD_KeyInfo_t {
    /* data key. */
    u64_t   key;
    /* support 1T data. */
    u64_t   data_offset:40;
    /* support 800M per record. */
    u64_t   data_length:24;
};

template<class KeyInfo_t>
class KeyIndex_t {
    public:
        /**
         *  constructor and destrutor.
         */
        KeyIndex_t(size_t inital_size=10000000):
            _num(0)
        {
            _data = (KeyInfo_t*)malloc(inital_size * sizeof(KeyInfo_t));
            _buffer_num = inital_size;
            _data_increase_num = inital_size / 5;
        }
        ~KeyIndex_t() {
            clear();
        }

        void append_key_info(KeyInfo_t x) {
            if (_buffer_num == 0) {
                size_t inital_size = 10000000;
                _data = (KeyInfo_t*)malloc(inital_size * sizeof(KeyInfo_t));
                _buffer_num = inital_size;
                _data_increase_num = inital_size / 5;

            } else if (_num >= _buffer_num) {
                _data = (KeyInfo_t*)realloc( _data, (_buffer_num + _data_increase_num) * sizeof(KeyInfo_t) );
                _buffer_num += _data_increase_num;
            }
            _data[_num ++] = x;
        }
        int make_index() {
            std::sort(_data, _data+_num, KeyInfo_cmp);
            return 0;
        }
        bool find(u64_t key, KeyInfo_t* out=NULL) const {
            KeyInfo_t info;
            info.key = key;
            size_t found_id;

            bool found = ratio_search(_data, _num, key, &found_id);
            if (found && out!=NULL) {
                *out = _data[found_id];
            }
            return found;
        }

        int read(FILE* fp) {
            fread(&_num, sizeof(_num), 1, fp);
            _data_increase_num = 1000000;
            _buffer_num = _num;
            _data = (KeyInfo_t*)malloc(_num * sizeof(KeyInfo_t));
            fread(_data, _num * sizeof(KeyInfo_t), 1, fp);
            return 0; 
        }

        int read(const char* filename) {
            clear();
            FILE* fp = fopen(filename, "r");
            read(fp);
            fclose(fp);
            return 0;
        }

        int write(FILE* fp) const {
            int ret;
            ret = fwrite(&_num, sizeof(_num), 1, fp);
            ret = fwrite(_data, _num * sizeof(KeyInfo_t), 1, fp);
            return 0;
        }

        int write(const char* filename) const {
            FILE* fp = fopen(filename, "w");
            write(fp);
            fclose(fp);
            return 0;
        }

        static bool KeyInfo_cmp(const KeyInfo_t& a, const KeyInfo_t& b) {
            return a.key < b.key;
        }

        static bool ratio_search(KeyInfo_t* arr, size_t num, u64_t key, size_t* out_index)
        {
            /*
            static size_t request_num = 0;
            static size_t proc_count = 0;
            request_num ++;
            fprintf(stderr, "%llu %llu %.3f\n", request_num, proc_count, float(proc_count) / request_num);
            */

            size_t begin = 0;
            size_t end = num;
            while (begin < end) {
                //proc_count ++;
#ifdef USE_RATIO_SEARCH
                size_t ratio_mid;
                if (begin + 8 < end) {
                    float ratio = float(key - arr[begin].key) / (arr[end-1].key - arr[begin].key);
                    ratio_mid = size_t(ratio * (end - 1 - begin)) + begin;
                    if (ratio_mid > end-1) {
                        ratio_mid = end - 1;
                    }
                } else {
                    ratio_mid = (end+begin) / 2;
                } 
#else
                size_t ratio_mid = (end+begin) / 2;
#endif
                //fprintf(stderr, "begin:%llu end:%llu mid:%llu\n", begin, end, ratio_mid);
                if (arr[ratio_mid].key < key) {
                    begin = ratio_mid + 1;
                } else if (arr[ratio_mid].key > key) {
                    end = ratio_mid;
                } else if (arr[ratio_mid].key == key) {
                    *out_index = ratio_mid;
                    return true;
                }
            }
            return false;
        }

        size_t size() const { return _num; }

        size_t memory_size() const { return _buffer_num * sizeof(KeyInfo_t); }

        void clear() {
            if (_data != NULL) {
                free(_data);
                _data = NULL;
            }
            _num = _buffer_num = 0;
        }

    private:
        KeyInfo_t *_data;
        size_t  _num;
        size_t  _buffer_num;
        size_t  _data_increase_num;
};

class Buffer_t {
    public:
        Buffer_t() {
            _buffer = (char*)malloc(InitialSize);
            _size = InitialSize;
            _used = 0;
        }
        ~Buffer_t() {
            release();
        }

        int release() {
            if (_buffer) {
                _used = 0;
                _size = 0;
                free(_buffer);
                _buffer = NULL;
            }
            return 0;
        }
        int append(const char* buffer, size_t length) {
            if (_size == 0) {
                _buffer = (char*)malloc(InitialSize);
                _size = InitialSize;
                _used = 0;

            } else if (_used + length >= _size) {
                size_t new_size = _size + GrowingSize;
                _buffer = (char*)realloc(_buffer, new_size);
                if (_buffer == NULL) {
                    return -1;
                }
                _size = new_size;
            }
            memcpy((char*)_buffer+_used, buffer, length);
            _used += length;
            return 0;
        }
        const char* buffer() const {
            return _buffer;
        }
        size_t length() const {
            return _used;
        }

        size_t memory_size() const {
            return _size;
        }

        int read(FILE* fp) {
            release();
            fread(&_used, sizeof(_used), 1, fp);
            fread(&_size, sizeof(_size), 1, fp);
            fprintf(stderr, "Buffer: reading _used=%llu _sized=%llu\n", _used, _size);
            _buffer = (char*)malloc(_size);
            fread(_buffer, _used, 1, fp);
            return 0;
        }
        int write(FILE* fp) const {
            fwrite(&_used, sizeof(_used), 1, fp);
            fwrite(&_size, sizeof(_size), 1, fp);
            fwrite(_buffer, _used, 1, fp);
            return 0;
        }

    private:
        char*   _buffer;
        size_t  _size;
        size_t  _used;
        static const size_t InitialSize = 32 * 1024 * 1024;
        static const size_t GrowingSize = 32 * 1024 * 1024;
};

class KVMemoryBatchDict_t {
    public:
        KVMemoryBatchDict_t() {
        }
        ~KVMemoryBatchDict_t() {
            _buffer.release();
        }

        int append(u64_t sign, const char* value, size_t value_size) {
            KVD_KeyInfo_t kinfo;
            kinfo.key = sign;
            kinfo.data_offset = _buffer.length();
            kinfo.data_length = value_size;

            _key_index.append_key_info(kinfo);
            _buffer.append(value, value_size);
            return 0;
        }
        int append_complete() {
            fprintf(stderr, "_key_index.num         : %ld\n", _key_index.size());
            fprintf(stderr, "_key_index.memory_size : %llum\n", _key_index.memory_size() / (1024*1024));
            fprintf(stderr, "_buffer.memory_size    : %llum\n", _buffer.memory_size() / (1024*1024));
            return _key_index.make_index();
        }
        
        /**
         *  return true if found.
         *  buffer_length:
         *      in : buffer_size.
         *      out: output value length. (<=buffer_size)
         */
        bool find(u64_t sign, char* buffer=NULL, size_t* buffer_length=NULL) {
            KVD_KeyInfo_t kinfo;
            if ( !_key_index.find(sign, &kinfo) ) {
                return false;
            }
            if (buffer == NULL) {
                /* no need to copy data. */
                return true;
            }
            if (*buffer_length < kinfo.data_length) {
                throw std::runtime_error("buffer size is too small.");
            }
            memcpy(buffer, _buffer.buffer() + kinfo.data_offset, kinfo.data_length);
            *buffer_length = kinfo.data_length;
            return true;
        }

        int read(FILE* fp) {
            int ret = _key_index.read(fp);
            if (ret<0) {
                fprintf(stderr, "Load index failed!\n");
                return ret;
            }
            return _buffer.read(fp);
        }
        int read(const char* filename) {
            FILE* fp = fopen(filename, "r");
            if (fp == NULL) {
                fprintf(stderr, "Open file [%s] to read failed.\n");
                return -1;
            }
            int ret = read(fp);
            fclose(fp);
            fprintf(stderr, "loaded num: %ld\n", _key_index.size());
            return ret;
        }
        int write(FILE* fp) const {
            int ret = _key_index.write(fp);
            if (ret<0) {
                return ret;
            }
            return _buffer.write(fp);
        }
        int write(const char* filename) const {
            FILE* fp = fopen(filename, "w");
            if (fp == NULL) {
                fprintf(stderr, "Open file [%s] to write failed.\n");
                return -1;
            }
            int ret = write(fp);
            fclose(fp);
            return ret;
        }

        size_t size() const { return _key_index.size(); }
        void clear() {
            _key_index.clear();
            _buffer.release();
        }

    private:
        KeyIndex_t<KVD_KeyInfo_t> _key_index;
        Buffer_t _buffer;
};

#endif // __KVINDEX_H__
