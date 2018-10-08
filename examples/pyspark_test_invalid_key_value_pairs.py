from pyspark import SparkContext

from invalid_key_value_pairs import job

logs = ['server0,1539015865,127.0.0.1,/index.html']

sc = SparkContext()
results = job(sc.parallelize(logs))
print(results)