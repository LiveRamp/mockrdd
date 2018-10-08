from mockrdd import MockRDD

from invalid_callable import count_distinct_servers

logs = ['server0,1539015865,127.0.0.1,/index.html',
        'server0,1539015866,127.0.0.1,/index.html']

results = count_distinct_servers(MockRDD.from_seq(logs))
print(results)
