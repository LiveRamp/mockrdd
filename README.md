# mockrdd
A Python module for testing PySpark code.

The MockRDD class offers similar behavior to pyspark.RDD with the following
extra benefits.
* Extensive sanity checks to identify invalid inputs
* Meaningful error messages for debugging issues
* Straightforward to running within pdb
* Removes Spark dependencies from development and testing environments
* No Spark overhead when running through large test suites

Simple example of using MockRDD in a test.
```python
from mockrdd import MockRDD

def job(rdd):
    return rdd.map(lambda x: x*2).filter(lambda x: x>3)
   
assert job(MockRDD.empty()).collect() == [] 
assert job(MockRDD.of(1)).collect() == [] 
assert job(MockRDD.of(2)).collect() == [4] 
```

Conventionally, you'd include a main method to hook the RDD up to product sources and sinks.
Further, the testing would be included in a separate file and use the module
`unitetest` for defining test cases.

See the docstring of `mockrdd.MockRDD` for more information.

## Copyright
Copyright 2018 LiveRamp

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.