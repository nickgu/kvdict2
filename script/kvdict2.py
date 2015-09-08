#! /bin/env python
# encoding=utf-8
# gusimiu@baidu.com
# 

import time
import sys
import c_kvdict2
import logging

class KVDict:
    def __init__(self):
        self.__dict_handle = c_kvdict2.create()
        logging.info('GET C_KVDICT : handle=%d' % self.__dict_handle)

    def load(self, filename):
        logging.info('LOAD: %s' % filename)
        print >> sys.stderr, 'loading: %s' % (filename)
        c_kvdict2.load(self.__dict_handle, filename, True )

    def find(self, key):
        return c_kvdict2.find(self.__dict_handle, key)

    def has(self, key):
        return c_kvdict2.has(self.__dict_handle, key)

    def write_bin(self, output_file):
        return c_kvdict2.write_mem_bin(self.__dict_handle, output_file)

    def load_bin(self, input_file):
        logging.info('LOAD: %s' % input_file)
        print >> sys.stderr, 'loading: %s' % (input_file)
        return c_kvdict2.load_mem_bin(self.__dict_handle, input_file)


class FileIndexKVDict:
    def __init__(self):
        self.__dict_handle = c_kvdict2.create()
        logging.info('GET C_KVDICT : handle=%d' % self.__dict_handle)

    def load(self, filename):
        c_kvdict2.load(self.__dict_handle, filename, False )

    def load_index(self, index_file, filename):
        return c_kvdict2.load_index_and_files(
                self.__dict_handle, index_file, filename)

    def write_index(self, output_file):
        return c_kvdict2.write_index( self.__dict_handle, output_file )

    def find(self, key):
        return c_kvdict2.find(self.__dict_handle, key)

    def has(self, key):
        return c_kvdict2.has(self.__dict_handle, key)


if __name__=='__main__':
    from pygsm.arg import *
    arg = Arg()
    arg.str_opt('bin', 'i', 'load bin list')
    arg.str_opt('filename', 'f', 'input text file.')
    arg.str_opt('dump_bin', 'b', 'dump memory dict to bin')
    arg.str_opt('calc_size', 's', 'calc the size of key [num]')
    opt = arg.init_arg()

    if opt.calc_size:
        key_num = int(opt.calc_size)
        print >> sys.stderr, 'KeyNum:%d * KeyInfoSize:16 bytes = %d = %d(M)' % (key_num, 16*key_num, 16*key_num/(2**20))
        sys.exit(0)

    d = KVDict()
    beg = time.time()
    if opt.bin:
        d.load_bin(opt.bin)
    else:
        d.load(opt.filename)
    if opt.filename is None and opt.bin is None:
        print >> sys.stderr, 'Argument error.'
        sys.exit(-1)

    end = time.time()
    during_time = end - beg
    logging.info('Load over. loading time = %.4f(s)' % during_time)

    if opt.dump_bin:
        t = time.time()
        d.write_bin(opt.dump_bin)
        logging.info('dumping time : %.4f(s)' % (time.time() - t))
        sys.exit(0)

    counter = 0
    beg = time.time()
    not_found = 0
    while 1:
        key = sys.stdin.readline();
        if key == '': break
        key = key.strip('\n');

        out = d.find(key);
        if out is None:
            not_found += 1
            print 'Not found.'
        else:
            print '%s: %s' % (key, out)
        counter += 1
    end = time.time()

    during_time = end - beg
    if counter>0:
        avg_time = during_time / counter
        logging.info('TIME      = %.4f(s)' % during_time)
        logging.info('COUNTER   = %4d' % counter)
        logging.info('AVERAGE   = %.4f(s)' % avg_time)
        logging.info('QPS       = %.1f(s)' % (counter / during_time))
        logging.info('NOTFOUND  = %4d(s)' % not_found)
