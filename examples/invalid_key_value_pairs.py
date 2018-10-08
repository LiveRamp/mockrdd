import typing
from pyspark import RDD

from util import parse_log_entry


def get_server_ip_uri(log_entry):
    return log_entry['server_name'], log_entry['remote_ip'], log_entry['uri']


def create_combiner(value) -> set:
    return {value}


def merge_value(values: set, value) -> set:
    values.add(value)
    return values


def merge_combiners(a: set, b: set) -> set:
    a.update(b)
    return a


def job(rdd: RDD) -> typing.List[typing.Tuple[str, int]]:
    return (rdd
            .map(parse_log_entry)
            .map(get_server_ip_uri)
            .combineByKey(create_combiner, merge_value, merge_combiners)
            .mapValues(len)
            .collect())
