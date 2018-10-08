# mockrdd
A Python3 module for testing PySpark code.

The MockRDD class offers similar behavior to [pyspark.RDD](http://spark.apache.org/docs/2.1.0/api/python/pyspark.html#pyspark.RDD) with the following
extra benefits.
* Extensive sanity checks to identify invalid inputs
* More meaningful error messages for debugging issues
* Straightforward to running within pdb
* Removes Spark dependencies from development and testing environments
* No Spark overhead when running through a large test suites

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
[unittest](https://docs.python.org/3/library/unittest.html) for defining test cases.

See the docstring of `mockrdd.MockRDD` for more information.
