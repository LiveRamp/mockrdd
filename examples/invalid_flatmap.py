from operator import itemgetter
from pyspark import RDD

from util import parse_log_entry


def count_distinct_timestamps(rdd: RDD) -> int:
    return (rdd
            .map(parse_log_entry)
            .flatMap(itemgetter('timestamp'))
            .distinct()
            .count())
