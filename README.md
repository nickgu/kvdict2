# kvdict2
key-value dict for python.

# Installation.
- change python header path in Makefile:
- make
- cd output; python benchmark.py 
    if run ok. you can see the bench mark QPS.


# How to use in your python code.
## Convert your text data to kvdict2 format.

you can use this command to do:
    > python kvdict2.py -f <your_text_input> -b <kvdict2_output_dict>

then if you want to check the dict. you can use this command.
    > python kvdict2.py -i <kvdict2_output_dict>
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


