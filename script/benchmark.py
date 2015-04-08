#! /bin/env python
# encoding=utf-8
# gusimiu@baidu.com
# 
#  对kvdict进行性能测试
#  

import os
import sys
import time
import random
import kvdict2

def random_string(n):
    ret = ('%1d' % random.randint(0, 10)) * n
    return ret

def test_file(d):
    d.load(file_name)
    tm_begin = time.time()
    for k in key_list:
        s = d.find(k)
    during = time.time() - tm_begin
    print >> sys.stderr, "SEARCHING_KEYS : %d" % len(key_list)
    print >> sys.stderr, "USING_TIME     : %.3f(s)" % during
    print >> sys.stderr, "AVERAGE_TIME   : %.3f(s)" % (during / len(key_list))
    print >> sys.stderr, "QPS            : %.1f qps" % (len(key_list) / during)

if __name__=='__main__':
    test_num = 1000000
    key_length = 8
    value_length = 128

    print >> sys.stderr, "preparing data.."
    file_name = 'benchmark_data.txt'
    os.system('rm -rf %s' % file_name)

    key_list = []
    f = file(file_name, 'w')
    for i in range(test_num):
        key = random_string(key_length)
        value = random_string(value_length)
        f.write('%s\t%s\n' % (key, value))
        key_list.append(key)
        if i % 100000 == 0:
            print >> sys.stderr, "write %d record(s)" % i
    f.close()
    print >> sys.stderr, "complete preparing."

    key_list = sorted(key_list)
    
    d = kvdict2.FileIndexKVDict()
    print >> sys.stderr, "KEY_LENGTH    : %d" % key_length
    print >> sys.stderr, "VALUE_LENGTH  : %d" % value_length
    print >> sys.stderr, "TEST #1 LOAD IN DISK:"
    test_file(d)
    print >> sys.stderr, "TEST #2 LOAD IN MEMORY:"
    d = kvdict2.KVDict()
    test_file(d)

    os.system('rm -rf benchmark_data.txt')
