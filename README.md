# KVdict2

## 作用
用于 Python 中加载内存词典的小工具，最主要作用是
 - 节省空间，对于大量python调研工作，python 读取数据的大小是不可接受的。这个词典可以以几乎等同于原始文件大小读入内存。
 - 快速建词典及加载，可以很容易构建一份key-value 词典，快速读取到内存，方便使用。

## 安装及使用方式
### 安装
- 修改 Makefile 中 PythonPath，对应到你的 python 库
- 执行make
- 测试：cd output; python benchmark.py 
如果执行ok，恭喜你

### 将数据读入
文件是tab 分割key 和value，则使用如下命令读入。
```
> python kvdict2.py -f <your_text_input> -b <kvdict2_output_dict>
```
然后可以用这个命令测试下是否符合预期
```
> python kvdict2.py -i <kvdict2_output_dict>
```
输入key，看看输出的value 是否符合预期。

### 在代码中如何使用。

代码中如下：
```
# import package.
import kvdict2

...
# initialize.
d = kvdict2.KVDict()
d.load_bin('your_kvdict2_dict')

# seek.
r = d.find(key)
if r None:
    # not found.
else:
    # do what you want to do.

```

## Summary
key-value dict for python.
## Installation.
- change python header path in Makefile:
- make
- cd output; python benchmark.py 
    if run ok. you can see the bench mark QPS.

# How to use in your python code.
## Convert your text data to kvdict2 format.

you can use this command to do:
```
> python kvdict2.py -f <your_text_input> -b <kvdict2_output_dict>
```

then if you want to check the dict. you can use this command.
```
> python kvdict2.py -i <kvdict2_output_dict>
```
input query to see if output is ok.

## Use dict in your python code.

you can use code as:
```
# import package.
import kvdict2

...
# initialize.
d = kvdict2.KVDict()
d.load_bin('your_kvdict2_dict')

# seek.
r = d.find(key)
if r None:
    # not found.
else:
    # do what you want to do.

```


