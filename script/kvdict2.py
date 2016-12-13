#! /bin/env python
# encoding=utf-8
# gusimiu@baidu.com
# 
"""notes
"""

import time
import sys
import os
import c_kvdict2
import logging

class KVDict(object):
    """notes
    """
    def __init__(self):
        """notes
        """
        self.__dict_handle = c_kvdict2.create()
        logging.info('GET C_KVDICT : handle=%d' % self.__dict_handle)

    def load(self, filename, is_bin=False):
        """notes
        """
        c_kvdict2.load(self.__dict_handle, filename, True, is_bin)

    def find(self, key):
        """notes
        """
        return c_kvdict2.find(self.__dict_handle, key)

    def has(self, key):
        """notes
        """
        return c_kvdict2.has(self.__dict_handle, key)

    def write_bin(self, output_file):
        """notes
        """
        return c_kvdict2.write_mem_bin(self.__dict_handle, output_file)

    def load_bin(self, input_file):
        """notes
        """
        return c_kvdict2.load_mem_bin(self.__dict_handle, input_file)


class FileIndexKVDict(object):
    """notes
    """
    def __init__(self):
        """notes
        """
        self.__dict_handle = c_kvdict2.create()
        logging.info('GET C_KVDICT : handle=%d' % self.__dict_handle)

    def load(self, filename, is_bin=False):
        """notes
        """
        c_kvdict2.load(self.__dict_handle, filename, False, is_bin)

    def load_index(self, index_file, filename):
        """notes
        """
        return c_kvdict2.load_index_and_files(
                self.__dict_handle, index_file, filename)

    def write_index(self, output_file):
        """notes
        """
        return c_kvdict2.write_index(self.__dict_handle, output_file)

    def find(self, key):
        """notes
        """
        return c_kvdict2.find(self.__dict_handle, key)

    def has(self, key):
        """notes
        """
        return c_kvdict2.has(self.__dict_handle, key)


class kvdict_disk(object):
    """notes
    """
    def __init__(self, file_path, index_file):
        """notes
        """
        self.__dct = FileIndexKVDict()
        if not os.path.exists(file_path):
            raise IOError("Dict File is not Exist! path:" + bin_file_path) 
        if not os.path.exists(index_file):
            raise IOError("Dict File is not Exist! path:" + index_file) 
        self.__dct.load_index(index_file, file_path)

    def __getitem__(self, key):
        """notes
        """
        r = self.__dct.find(key)
        if r is None:
            #raise KeyError(key)
            return None
        return r

    def get(self, key, default=None):
        """notes
        """
        r = self.__dct.find(key)
        if r is None:
            return default
        return r

    def __contains__(self, key):
        """notes
        """
        return self.has_key(key)

    def has_key(self, key):
        """notes
        """
        r = self.__dct.find(key)
        if r is None:
            return False
        return True


class kvdict(object):
    """notes
    """
    def __init__(self, file_path):
        """notes
        """
        self.__dct = KVDict()
        if not os.path.exists(file_path):
            raise IOError("Dict File is not Exist! path:" + file_path) 
        if file_path.endswith(".kvd2"):
            self.__dct.load_bin(file_path)
        else:
            self.__dct.load(file_path)

    def __getitem__(self, key):
        """notes
        """
        r = self.__dct.find(key)
        if r is None:
            #raise KeyError(key)
            return None
        return r

    def get(self, key, default=None):
        """notes
        """
        r = self.__dct.find(key)
        if r is None:
            return default
        return r

    def __contains__(self, key):
        """notes
        """
        return self.has_key(key)

    #@profile
    def has_key(self, key):
        """notes
        """
        r = self.__dct.find(key)
        if r is None:
            return False
        return True



if __name__ == '__main__':
    try:
        from pygsm.arg import *
    except:
        from arg import *
    import base64
    arg = Arg()
    arg.str_opt('bin', 'i', 'load bin list')
    arg.str_opt('filename', 'f', 'input text file.')
    arg.str_opt('dump_bin', 'b', 'dump memory dict to bin')
    arg.str_opt('dump_disk', 'd', 'dump memory dict to disk')
    arg.str_opt('calc_size', 's', 'calc the size of key [num]')
    arg.str_opt('load_bin', 'o', '')
    opt = arg.init_arg()

    if opt.calc_size:
        key_num = int(opt.calc_size)
        print >> sys.stderr, 'KeyNum:%d * KeyInfoSize:16 bytes = %d = %d(M)' % (
                key_num, 16 * key_num, 16 * key_num / (2 ** 20))
        sys.exit(0)
    is_bin = False
    if opt.load_bin:
        is_bin = True

    if opt.dump_disk:
        d = FileIndexKVDict()
    else:
        d = KVDict()
    beg = time.time()
    if opt.bin:
        if opt.bin.endswith(".idx"):
            d = FileIndexKVDict()
            d.load_index(opt.bin, opt.bin.replace(".idx", ""))
        elif opt.bin.endswith(".kvd2"):
            d.load_bin(opt.bin)
        else:
            d.load(opt.bin, is_bin)
    else:
        d.load(opt.filename, is_bin)
    if opt.filename is None and opt.bin is None:
        print >> sys.stderr, 'Argument error.'
        sys.exit(-1)

    end = time.time()
    during_time = end - beg
    logging.info('Load over. loading time = %.4f(s)' % during_time)
    if opt.dump_disk:
        t = time.time()
        d.write_index(opt.dump_disk)
        logging.info('dumping time : %.4f(s)' % (time.time() - t))
        sys.exit(0)

    if opt.dump_bin:
        t = time.time()
        d.write_bin(opt.dump_bin)
        logging.info('dumping time : %.4f(s)' % (time.time() - t))
        sys.exit(0)

    counter = 0
    beg = time.time()
    not_found = 0
    while 1:
        line = sys.stdin.readline().decode('utf8').encode('gb18030')
        if line == '':
            break
        key = line.strip('\n').split("\t")[0]

        out = d.find(key)
        if out is None:
            not_found += 1
            print 'Not found.'
            continue
        else:
            if is_bin:
                out = base64.b64encode(out)
            print ('%s\t%s' % (key, out)).decode('gb18030').encode('utf8')
            #sys.stdout.write(line)
        counter += 1
    end = time.time()

    during_time = end - beg
    if counter > 0:
        avg_time = during_time / counter
        logging.info('TIME      = %.4f(s)' % during_time)
        logging.info('COUNTER   = %4d' % counter)
        logging.info('AVERAGE   = %.4f(s)' % avg_time)
        logging.info('QPS       = %.1f(s)' % (counter / during_time))
        logging.info('NOTFOUND  = %4d(s)' % not_found)
