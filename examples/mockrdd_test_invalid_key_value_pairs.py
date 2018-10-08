from mockrdd import MockRDD

from invalid_key_value_pairs import job

logs = ['server0,1539015865,127.0.0.1,/index.html']

results = job(MockRDD.from_seq(logs))
print(results)