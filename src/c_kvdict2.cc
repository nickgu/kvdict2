/**
 * @file c_kvdict2.cc
 * @author nickgu
 * @date 2013/06/26 14:15:34
 * @brief 
 *      用Python的C接口实现一个词典类。
 *      可以弥补Python自带词典性能问题。
 *       内存KV词典：
 *       - 将文件读入Key-Value信息（目前局限都是文本）
 *       - 将哈希信息序列化到文件（方便在此调用）
 *       硬盘KV索引词典
 *       - 构建大文件的索引
 *  
 **/
#include <python2.7/Python.h> //包含python的头文件

#include <map>
#include <string>
#include <vector>
#include <ext/hash_map>
#include <ext/hash_set>

#include "kvindex.h"

using namespace __gnu_cxx;
using namespace std;

typedef KeyIndex_t<KVD_KeyInfo_t> FileOffsetDict_t;

/**
 *  KVDict
 */
struct KVDict_t {
    // 当前词典是否是内存词典
    bool memory_mode;

    // 内存词典结构
    KVMemoryBatchDict_t memory_dict;

    // 硬盘词典结构
    char buffer[1024*128];
    FileOffsetDict_t disk_dict;
    FILE* fp;
};
vector<KVDict_t*> g_dict_pool;

unsigned int Mod_Prime_List_1[256]={
       256,     65536,         3,       768,    196608,         9,      2304,    589824,
        27,      6912,   1769472,        81,     20736,   5308416,       243,     62208,
  15925248,       729,    186624,  14221318,      2187,    559872,   9109528,      6561,
   1679616,  10551371,     19683,   5038848,  14876900,     59049,  15116544,  11076274,
    177147,  11795206,  16451609,    531441,   1831192,  15800401,   1594323,   5493576,
  13846777,   4782969,  16480728,   7985905,  14348907,  15887758,   7180502,   9492295,
  14108848,   4764293,  11699672,   8772118,  14292879,   1544590,   9539141,   9324211,
   4633770,  11840210,  11195420,  13901310,   1966204,     31834,   8149504,   5898612,
     95502,   7671299,    918623,    286506,   6236684,   2755869,    859518,   1932839,
   8267607,   2578554,   5798517,   8025608,   7735662,    618338,   7299611,   6429773,
   1855014,   5121620,   2512106,   5565042,  15364860,   7536318,  16695126,  12540154,
   5831741,  16530952,   4066036,    718010,  16038430,  12198108,   2154030,  14560864,
   3039898,   6462090,  10128166,   9119694,   2609057,  13607285,  10581869,   7827171,
   7267429,  14968394,   6704300,   5025074,  11350756,   3335687,  15075222,    497842,
  10007061,  11671240,   1493526,  13243970,   1459294,   4480578,   6177484,   4377882,
  13441734,   1755239,  13133646,   6770776,   5265717,   5846512,   3535115,  15797151,
    762323,  10605345,  13837027,   2286969,  15038822,   7956655,   6860907,  11562040,
   7092752,   3805508,   1131694,   4501043,  11416524,   3395082,  13503129,    695146,
  10185246,   6954961,   2085438,  13778525,   4087670,   6256314,   7781149,  12263010,
   1991729,   6566234,   3234604,   5975187,   2921489,   9703812,   1148348,   8764467,
  12334223,   3445044,   9516188,   3448243,  10335132,  11771351,  10344729,  14228183,
   1759627,  14256974,   9130123,   5278881,   9216496,  10613156,  15836643,  10872275,
  15062255,  13955503,  15839612,  11632339,   8312083,  13964410,   1342591,   8159036,
   8338804,   4027773,   7699895,   8239199,  12083319,   6322472,   7940384,   2695531,
   2190203,   7043939,   8086593,   6570609,   4354604,   7482566,   2934614,  13063812,
   5670485,   8803842,   5637010,    234242,   9634313,    133817,    702726,  12125726,
    401451,   2108178,   2822752,   1204353,   6324534,   8468256,   3613059,   2196389,
   8627555,  10839177,   6589167,   9105452,  15740318,   2990288,  10539143,  13666528,
   8970864,  14840216,   7445158,  10135379,  10966222,   5558261,  13628924,  16121453,
  16674783,   7332346,  14809933,  16469923,   5219825,  10875373,  15855343,  15659475,
  15848906,  14011603,  13423999,  13992292,   8480383,   6717571,   8422450,   8663936,
   3375500,   8490137,   9214595,  10126500,   8693198,  10866572,  13602287,   9302381
};

unsigned int Mod_Prime_List_2[256]={
  15824477,   7761153,   7145686,    580925,  14499208,   4036269,   9875725,  11605750,
   1507777,    115335,  12748561,   8855010,   1960695,  15399149,  16317578,  16554616,
  10127548,   8963642,  12993288,   4396326,   1387123,   2782309,   7628746,   6803892,
  13744855,  12248289,  15002970,  15558948,   6894525,   3392505,  12844131,  16543731,
   7340988,    246640,  12808243,   7356403,   4192880,  16413743,   7618458,   4170164,
  10598447,  12073393,   3783992,  12401609,   3921293,  13996267,   9500965,  16330384,
   3055753,  10521614,   9181344,   1616204,  11095448,   5088057,  10698269,   4073427,
   2610974,  14098583,   2139463,  10832160,   4795125,   2816473,  16374730,  14408329,
  14325643,   9935226,  10060807,   8655145,   1126852,   3261729,  12919873,   2379285,
   5117796,   1534254,   6893447,   3116537,   9305119,  16525405,   2649532,   7192232,
  12496701,  11487646,   4827551,  11117529,  10740793,  14959571,   4448804,  14821491,
   2654722,   8520872,    307362,  11575876,  10637232,   5225154,  12240703,  13060954,
   4941623,   6765563,   3932631,    121596,  14351377,  16523130,   2067132,   9092623,
  12458026,   1586846,   3579800,  10460054,  10199183,  10525003,  10048928,   5614121,
  11153061,   3059786,  11554062,   5052848,   1684765,  11869865,   2012421,  11863806,
    461317,    656759,    358314,   7842389,  11164903,   6091338,  15880220,   5254162,
   2889552,   1528556,   5434759,  15567986,   9208253,   8504908,  12997777,   5545510,
  10365844,   2858622,  10387675,   8447358,  15042176,   8818485,   9387494,   4059007,
  15696653,   8592607,   1894323,  15185116,  11856727,  15426292,   6488987,    237971,
  10588979,   9649585,   4045507,  12240653,  13048154,   1664823,   6764713,   3715031,
  11524792,  14336927,  12823930,  11372275,   8846973,  16680422,   8779486,  16180949,
  15131990,  15033670,   6640949,   5585845,   3914405,  12232939,  11073370,  16213288,
   6633575,   3698101,   7190712,  12107581,  12536120,   4801711,   4502489,  11787652,
  14520291,   9433517,  15840895,  11964161,   9374998,    860031,   2064349,   8380175,
  14620527,   1539535,   8245383,  13668173,   9394896,   5953919,  14255354,   8718441,
    553429,   7460232,  13995905,   9408293,   9383551,   3049599,   8946190,   8525576,
   1511586,   1090439,  10717200,   8919763,   1760264,  14420410,    641180,  13147289,
  10266184,  10900060,   5400326,   6753138,    751831,   7919547,  14140152,  12781127,
    414707,   5501798,  15952771,   7050019,   9644571,   2761923,   2409930,  12962916,
  13398293,   7414412,   2265985,   9667394,   8604611,   4967347,  13350907,  12060795,
    558904,   8861832,   3707127,   9501368,  16433552,  12689562,  10528465,  10935200,
  14396166,  11211915,   1349211,   9854036,   6053366,   6159388,  16523821,   2244028
};

unsigned int getsign_24_1(char* str)
{
    int i=0;
   	unsigned int itemp;

    itemp=0;

    while (str[i]!=0)
       	itemp=((unsigned char)str[i]*Mod_Prime_List_1[0xFF & (i++)]+itemp); //% PRIME_USED_24;
    return itemp;
}

unsigned int getsign_24_2(char* str)
{

    int i=0;
    unsigned int itemp;

    itemp=0;

    while (str[i]!=0)
        itemp=((unsigned char)str[i]*Mod_Prime_List_2[0xFF & (i++)]+itemp); //% PRIME_USED_24;
    return itemp;
}

int creat_sign_f64(char* psrc,int slen,unsigned int* sign1,unsigned int * sign2)
{
    assert( strlen(psrc) == (unsigned int) slen);
    *sign1=0;
    *sign2=0;
    if( slen <= 4 )
    {
        memcpy(sign1,psrc,slen);
        return 1;
    }
    else 
        if(slen<=8)
        {
            memcpy(sign1,psrc,4);
            memcpy(sign2,psrc+4,slen-4);
            return 1;
        }
        else
        {
            (*sign1)=getsign_24_1(psrc);
            (*sign2)=getsign_24_2(psrc);
            return 1;
        }
}


/**
 *  封装签名函数
 */
static size_t calc_sign(const char* s) {
    unsigned slen = strlen(s);
    unsigned low;
    unsigned high;
    creat_sign_f64((char*)s, slen, &low, &high);
    size_t ret = high;
    ret = (ret << 32LL) | low;
    return ret;
};

/**
 *  封装签名操作
 */
struct Sign_t {
    size_t operator() (const string& s) const {
        return calc_sign(s.c_str());
    }
};

/*
 * 随机访问文件获取一个数据
 */
int fgets_random(char* buffer,
                size_t buffer_size, 
                KVD_KeyInfo_t kv_info,
                FILE* fp)
{
    int ret = fseek(fp, kv_info.data_offset, SEEK_SET); 
    if (ret<0) {
        return ret;
    }
    buffer[ kv_info.data_length ] = 0;
    return fread(buffer, kv_info.data_length, 1, fp);
}

/**
 *  加载文件到词典
 */
int load(int dict_id, 
        const char* filename, 
        bool load_in_memory) 
{
    KVDict_t* pdict = g_dict_pool[dict_id];
    pdict->memory_mode = load_in_memory;
    //fprintf(stderr, "FCNT=%d LOADMEMORY=%d\n", fcnt, load_in_memory);
    char* line = pdict->buffer;
    size_t MAX_LINE_LENGTH = sizeof(pdict->buffer);
    unsigned counter = 0;
    FILE* fp = fopen(filename, "r");
    if (fp==NULL) {
        fprintf(stderr, "Cannot open file : %s\n", filename);
        return -1;
    }
    size_t fpos_b = ftell(fp);
    while (fgets(line, MAX_LINE_LENGTH, fp)) {
        unsigned line_len = strlen(line);
        // strip.
        line[line_len-1] = 0;
        size_t fpos_e = ftell(fp);

        size_t sign;
        const char* value_pos = "";
        unsigned value_length = 0;
        unsigned value_offset = 0;
        for (int i=0; line[i]; ++i) {
            if (line[i]=='\t') {
                line[i] = 0;
                value_pos = line+i+1;
                value_offset = i+1;
                value_length = line_len - i - 1;
                break;
            }
        }
        // get key-sign.
        sign = calc_sign(line);

        // 每一行都会被录入，如果只有一列，则整列作为key，value为空
        if (pdict->memory_mode) {
            // insert into memory dict.
            pdict->memory_dict.append(sign, value_pos, value_length+1);

        } else {
            KVD_KeyInfo_t d;
            d.key = sign;
            d.data_offset = fpos_b + value_offset;
            d.data_length = fpos_e - 1 - fpos_b - value_offset;
            // insert into file offset dict.
            pdict->disk_dict.append_key_info(d);
        }
        fpos_b = fpos_e;

        counter ++;
        if (counter % 1000000==0) {
            fprintf(stderr, "Load %d records over.\n", counter);
        }
    }
    if (pdict->memory_mode) {
        fprintf(stderr, "Begin to make index..\n");
        pdict->memory_dict.append_complete();
        fprintf(stderr, "make index over.\n");
        fclose(fp);
    } else {
        fprintf(stderr, "Begin to make index..\n");
        pdict->disk_dict.make_index();
        fprintf(stderr, "make index over.\n");
        pdict->fp = fp;
    }
    fprintf(stderr, "Load dict over! memory_records=%u disk_records=%u\n", 
            pdict->memory_dict.size(), 
            pdict->disk_dict.size());
    return 0;
}

int has(int dict_id, const char* k) 
{
    KVDict_t* pdict = g_dict_pool[dict_id];
    string key = k;
    size_t sign = calc_sign(key.c_str());
    if (pdict->memory_mode) {
        if ( !pdict->memory_dict.find(sign) ) {
            return 0;
        }
    } else {
        if ( !pdict->disk_dict.find(sign) ) {
            return 0;
        }
    }
    return 1;
}

int seek(int dict_id, 
        const char* k, 
        string& out) 
{
    KVDict_t* pdict = g_dict_pool[dict_id];
    string key = k;
    size_t sign = calc_sign(key.c_str());
    if (pdict->memory_mode) {
        size_t out_size = sizeof(pdict->buffer);
        if ( !pdict->memory_dict.find(sign, pdict->buffer, &out_size) ) {
            return 0;
        }
        out = string(pdict->buffer);
    } else {
        KVD_KeyInfo_t key_info;
        if ( !pdict->disk_dict.find(sign, &key_info) ) {
            return 0;
        }
        fgets_random(pdict->buffer, sizeof(pdict->buffer), key_info, pdict->fp);
        out = string(pdict->buffer);
    }
    return 1;
}

static PyObject * wrapper_create(PyObject *self, PyObject *args)  {
    int did = g_dict_pool.size();
    // create new.
    g_dict_pool.push_back(new KVDict_t());
    return Py_BuildValue("i", did);
}

// 2 python 包装
static PyObject * wrapper_load(PyObject *self, PyObject *args) 
{
    int did = PyInt_AsLong(PyTuple_GetItem(args, 0));
    PyObject *fn = PyTuple_GetItem(args, 1);
    char* filename = PyString_AsString(fn);
    bool load_in_memory = (bool)PyInt_AsLong(PyTuple_GetItem(args, 2));

    int ret = load(did, filename, load_in_memory);
    return Py_BuildValue("i", ret);//把c的返回值n转换成python的对象
}

/**
 * 将内存词典写到文件
 */
static PyObject * wrapper_write_mem_bin(PyObject *self, PyObject *args) {
    int did = PyInt_AsLong(PyTuple_GetItem(args, 0));
    char* output_filename = PyString_AsString(PyTuple_GetItem(args, 1));

    fprintf(stderr, "begin writing memory_dict [dict_id:%d] to [%s]...\n", did, output_filename);
    if (did<0 || did>=(int)g_dict_pool.size()) {
        return Py_BuildValue("i", -1);
    }

    KVDict_t* dict = g_dict_pool[did];
    dict->memory_dict.write(output_filename);
    return Py_BuildValue("i", 0);//把c的返回值n转换成python的对象
}

static PyObject * wrapper_load_mem_bin(PyObject *self, PyObject *args)
{
    int did = PyInt_AsLong(PyTuple_GetItem(args, 0));
    char* dict_name = PyString_AsString(PyTuple_GetItem(args, 1));
    if (did<0 || did>=(int)g_dict_pool.size()) {
        return Py_BuildValue("i", -1);
    }
    KVDict_t* dict = g_dict_pool[did];
    dict->memory_mode = true;
    dict->memory_dict.clear();
    dict->memory_dict.read(dict_name);
    fprintf(stderr, "Memory dict load over. [%llu] records loaded.\n", dict->memory_dict.size());
    return Py_BuildValue("i", 0);//把c的返回值n转换成python的对象
}

/**
 *  将硬盘词典的索引写到文件中
 */
static PyObject * wrapper_write_index(PyObject *self, PyObject *args)
{
    int did = PyInt_AsLong(PyTuple_GetItem(args, 0));
    char* output_filename = PyString_AsString(PyTuple_GetItem(args, 1));

    fprintf(stderr, "Writting file index: %d, out_fn: %s\n", did, output_filename);
    if (did<0 || did>=(int)g_dict_pool.size()) {
        return Py_BuildValue("i", -1);
    }
    KVDict_t* dict = g_dict_pool[did];
    dict->disk_dict.write(output_filename);
    return Py_BuildValue("i", 0);//把c的返回值n转换成python的对象
}

static PyObject * wrapper_load_index_and_file(PyObject *self, PyObject *args)
{
    int did = PyInt_AsLong(PyTuple_GetItem(args, 0));
    const char* index_file_name = PyString_AsString(PyTuple_GetItem(args, 1));
    const char* filename = PyString_AsString(PyTuple_GetItem(args, 2)); 

    if (did<0 || did>=(int)g_dict_pool.size()) {
        return Py_BuildValue("i", -1);
    }
    KVDict_t* dict = g_dict_pool[did];
    dict->memory_mode = false;
    dict->fp = NULL;
    dict->disk_dict.clear();

    fprintf(stderr, "dict_id:%d, index_file_name:%s, file:%s\n", did, index_file_name, filename);
    dict->fp = fopen(filename, "r");
    dict->disk_dict.read(index_file_name);
    fprintf(stderr, "Disk dict load over. [%llu] records loaded.\n", dict->disk_dict.size());
    return Py_BuildValue("i", 0);//把c的返回值n转换成python的对象
}

static PyObject * wrapper_seek(PyObject *self, PyObject *args) {
    int did = PyInt_AsLong(PyTuple_GetItem(args, 0));
    const char* key = PyString_AsString(PyTuple_GetItem(args, 1));
    if (key == NULL) {
        fprintf(stderr, "KVDict: parse input key failed! key is NULL.");
        Py_INCREF(Py_None);
        return Py_None;
    }
    string out;
    int found = seek(did, key, out);
    if (found) {
        return Py_BuildValue("s", out.c_str());
    } else {
        Py_INCREF(Py_None);
        return Py_None;
    }
}

static PyObject * wrapper_has(PyObject *self, PyObject *args) {
    int did = PyInt_AsLong(PyTuple_GetItem(args, 0));
    const char* key = PyString_AsString(PyTuple_GetItem(args, 1));
    int found = has(did, key);
    return Py_BuildValue("i", found);
}

// 3 方法列表
static PyMethodDef CKVDictFunc[] = {
    // 创建一个词典
    { "create", wrapper_create, METH_VARARGS, "create a dict."},
    // 读取文件到词典，可选是否是内存结构
    { "load", wrapper_load, METH_VARARGS, "load files into dict."},
    // 查找信息
    { "find", wrapper_seek, METH_VARARGS, "search dict. return None if not exists."},
    // 是否包含特定key
    { "has", wrapper_has, METH_VARARGS, "check key in dict."},
    // 将内存词典序列化到文件
    { "write_mem_bin", wrapper_write_mem_bin, METH_VARARGS, "write mem-dict to bin file." },
    // 读取序列化好的文件
    { "load_mem_bin", wrapper_load_mem_bin, METH_VARARGS, "load mem-dict to bin file." },
    // 将索引写到硬盘
    { "write_index",  wrapper_write_index, METH_VARARGS, "write dict to index-file."},
    // 将索引和对应的文件读入到硬盘词典
    { "load_index_and_files", wrapper_load_index_and_file, METH_VARARGS, "load index and files to memory." },
    { NULL, NULL, 0, NULL }
};
// 4 模块初始化方法
PyMODINIT_FUNC initc_kvdict2(void) {
    //初始模块，把CKVDictFunc初始到c_kvdict中
    PyObject *m = Py_InitModule("c_kvdict2", CKVDictFunc);
    if (m == NULL)
        return;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
