from pyspark import SparkContext

from invalid_flatmap import count_distinct_timestamps

logs = ['server0,1539015865,127.0.0.1,/index.html',
        'server0,1539015866,127.0.0.1,/index.html']

sc = SparkContext()
results = count_distinct_timestamps(sc.parallelize(logs))
print(results)
