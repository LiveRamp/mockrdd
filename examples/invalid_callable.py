from operator import itemgetter
from pyspark import RDD

from util import parse_log_entry


def count_distinct_servers(rdd: RDD) -> int:
    return (rdd
            .map(parse_log_entry)
            .map('server_name')  # should be itemgetter('server_name')
            .distinct()
            .count())
