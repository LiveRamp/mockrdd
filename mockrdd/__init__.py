# Copyright 2018 LiveRamp
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the “Software”), to deal in
# the Software without restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
# Software, and to permit persons to whom the Software is furnished to do so, subject
# to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies
# or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import random
import copy
import typing
import warnings
from collections import Counter, defaultdict
from functools import partial, reduce
from itertools import chain

__all__ = ['MockRDD']

RANDOM_ARG_TYPE = typing.Union[type(None), bool, int, random.Random]


def make_random(rnd: RANDOM_ARG_TYPE) -> typing.Optional[random.Random]:
    """create a random number generating using several different types of inputs

    :param rnd: Either None, False, an integer seed, or an instance of random.Random
    :return: None or an instance of random.Random
    """
    if rnd is None:
        return random.Random()
    elif rnd is False:
        return None
    elif isinstance(rnd, int):
        return random.Random(rnd)
    elif isinstance(rnd, random.Random):
        return rnd
    else:
        raise TypeError("Can't generate a random from " + repr(rnd))


def unpack_key_value_pair(entry) -> typing.Tuple[object, object]:
    """ensure the given entry can be unpacked into a key/value pair

    :param entry: an iterable element with exactly two elements
    :return: a tuple containing the key value pair
    :raises: ValueError when the entry can't be unpacked
    """
    try:
        k, v = entry
    except (TypeError, ValueError, AttributeError):
        raise ValueError(f"{entry} is not a key/value pair")
    return k, v


def check_iterable(obj) -> typing.Iterator:
    try:
        return iter(obj)
    except (TypeError, ValueError, AttributeError):
        raise TypeError(f"{obj} is not iterable")


def check_callable(obj) -> typing.Callable:
    if not callable(obj):
        raise TypeError(f"{obj} is not callable")
    return obj


def check_partitions(num_partitions: typing.Optional[int]):
    if num_partitions is not None:
        if not isinstance(num_partitions, int):
            raise TypeError(f"{num_partitions} is not an integer")
        if num_partitions <= 0:
            raise ValueError(f"bad number of partitions {num_partitions}")


def check_optional_callable(obj) -> typing.Optional[typing.Callable]:
    return check_callable(obj) if obj is not None else None


def run_combine_by_key(entries: typing.Iterator[typing.Tuple[object, object]],
                       create_combiner: typing.Callable[[object], object],
                       merge_value: typing.Callable[[object, object], object],
                       merge_combiners: typing.Callable[[object, object], object],
                       copier: typing.Optional[typing.Callable[[object], object]] = None,
                       rnd: RANDOM_ARG_TYPE = None) -> typing.ItemsView[object, object]:
    """Run the logic of RDD.combineByKey. Performs assorted sanity checks.

    :param entries: The key/value inputs
    :param create_combiner: A callable to create a combiner state
    :param merge_value: A callable to add a value to the combiner state
    :param merge_combiners: A callable to merge two combiners
    :param copier: An optional callable to copy values
    :param rnd: An optional random state to change the order in which key/value entries are processed
    :return: Pairs of key and final aggregate state
    """
    if copier is None:
        copier = copy.deepcopy

    entries_copy = []
    for entry in entries:
        k, v = unpack_key_value_pair(entry)
        entries_copy.append((k, copier(v)))

    rnd = make_random(rnd)
    if rnd is not None:
        entries_copy = list(entries_copy)
        rnd.shuffle(entries_copy)

    combiners = {}
    # It's important that we test both the value merger and the combiner one
    # To facilitate that, the following variable is used to switch back and forth
    merge_combiners_instead_of_value = False
    for entry in entries_copy:
        k, v = unpack_key_value_pair(entry)
        if k not in combiners:
            combiners[k] = create_combiner(v)
        elif not merge_combiners_instead_of_value:
            combiners[k] = merge_value(combiners[k], v)
            merge_combiners_instead_of_value = True
        else:
            combiners[k] = merge_combiners(combiners[k], create_combiner(v))
            merge_combiners_instead_of_value = False
    return combiners.items()


def identity(x):
    return x


def add_value_type(rdd: 'MockRdd', value_type: object) -> 'MockRDD':
    return rdd.mapValues(lambda x: (value_type, x))


def chunk(itr: typing.Iterator, chunk_size: int) -> typing.Iterator[typing.List]:
    """break an iterator into multiple chunks of a specified size

    :param itr: any iterable
    :param chunk_size: the size of each chunk
    :return: a generator yielding lists of size chunk_size (or less for the last tone)
    """

    assert chunk_size > 0
    itr = iter(itr)
    while True:
        c = []
        try:
            while len(c) < chunk_size:
                c.append(next(itr))
        except StopIteration:
            if c:
                yield c
            break
        yield c


class MockRDD:
    """Mock the behavior of PySpark RDD for use in testing.

    Like the real RDD, the MockRDD consists of a collection of values onto which
    operations are performed; commonly to crete a new MockRDD. Each MockRDD has a
    sequence of input values and a function that operates on them to produces a
    new sequence.

    A MockRDD itself can be a sequence of values and thereby can be used as input
    to a new MockRDD. In fact, that is how we emulate the behavior of PySpark;
    when you call a method such as `map` or `filter` a new MockRDD is formed by
    wrapping the current MockRDD into a new one along with a function that performs the
    desired operations.

    The general use case is for testing PySpark computation to find bugs and verify the
    correctness of computing the desired results. For example, say I wanted to compute
    the number of distinct maintained PELs in a RDD containing dictionaries with 'pel'
    attributes. I would start by writing a function that performs the operations from
    an input RDD.

    def count_unique_mpels(input_rdd):
        # Note this is a bad way to compute cardinality. Instead you should use
        # lr_py_utils.adaptive_cardinality.AdaptiveCardinality
        return (input_rdd
                .map(lambda x: x['pel'])
                .filter(lambda p: p.startswith('XY'))
                .distinct()
                .count())

    Now that I have a function for building the operations on an input RDD, I can pass in a
    mock RDD filled with test data and verify that I get the expected results. Very simple
    input is used to make verification trivial.

    # Ensure the operations handles the empty input case
    assert count_unique_mpels(MockRDD.empty()) == 0

    # Ensure we can recognize a single mPEL
    assert count_unique_mpels(MockRDD.from_seq(['XY1005ABC'])) == 1

    # Ensure we're not counting dPELs
    assert count_unique_mpels(MockRDD.from_seq(['Xi1005ABC'])) == 0

    # Ensure we're not doubling counting the same mPEL
    assert count_unique_mpels(MockRDD.from_seq(['XY1005ABC', 'XY1005ABC'])) == 1

    # Ensure we can count more than one
    assert count_unique_mpels(MockRDD.from_seq(['XY1005ABC', 'XY100XYZ'])) == 2

    # Ensure the combination of mPELs and dPELs doesn't cause any problems
    assert count_unique_mpels(MockRDD.from_seq(['XY1005ABC', 'Xi1005ABC'])) == 1

    This may appear to be a trivial example, but I've commonly made small typos that weren't
    triggered until the workflow had already been running for a while. This is a major
    downside to using a language that doesn't resolve symbols at compile time. (Technically
    Python does, but by default doesn't warn if you used an undefined global variable.
    There are good Python code analysis tools such as pylint that can catch these bugs)

    Additionally, we can break up our complicated PySpark operations into a sequence of
    simpler ones and verify the correctness of each operations more easily. We can have
    a very simple final test that combines everything to ensure the interface between
    groups of operations are correct.
    """

    def __init__(self,
                 func: typing.Callable,
                 source_seq: typing.Union[typing.Iterator, typing.Iterable],
                 rnd: RANDOM_ARG_TYPE = None,
                 allow_multiple_passes: bool = False):
        """Internal constructor. Use the classmethods `from_seq`, `empty`, and `of` to instantiate
        a MockRDD

        :param func: an function performed on source_seq to create the output for this RDD
        :param source_seq: an sequence of input value to this RDD
        :param rnd: a random number generator for used to introduce non-deterministic behavior.
                    Can be disabled by passing False.
        :param allow_multiple_passes: by default a MockRDD can only be ran once, but `persist()`
                    allows this to be overridden.
        """
        self.func = func
        self.source_seq = source_seq
        if rnd is None:
            if isinstance(source_seq, MockRDD):
                assert source_seq.rnd is not None
                rnd = source_seq.rnd
        self.rnd = make_random(rnd)
        self.allow_multiple_passes = bool(allow_multiple_passes)
        self.ran = False

    @classmethod
    def from_seq(cls, seq: typing.Union[typing.Iterator, typing.Iterable]) -> 'MockRDD':
        return cls(identity, seq)

    @classmethod
    def empty(cls) -> 'MockRDD':
        return cls.from_seq(())

    @classmethod
    def of(cls, *args) -> 'MockRDD':
        return cls.from_seq(args)

    def __iter__(self):
        if self.ran and not self.allow_multiple_passes:
            warnings.warn("attempt to read from a non-persisted MockRDD multiple times")
        self.ran = True
        return check_iterable(self.func(self.source_seq))

    def collect(self) -> list:
        return list(self)

    def collectAsMap(self) -> dict:
        return dict(self.collect())

    def cache(self) -> 'MockRDD':
        return self.persist()

    def _chain(self, func, allow_multiple_passes=None) -> 'MockRDD':
        if allow_multiple_passes is None:
            allow_multiple_passes = self.allow_multiple_passes
        return self.__class__(func, self, rnd=self.rnd, allow_multiple_passes=allow_multiple_passes)

    def persist(self, storageLevel=None) -> 'MockRDD':
        """Ensure that the MockRDD can be called multiple times, regardless of its input source
        """
        results = None

        def wrapper(source_seq):
            nonlocal results
            if results is None:
                results = list(source_seq)
            return results

        return self._chain(wrapper, allow_multiple_passes=True)

    def unpersist(self):
        return self._chain(identity, allow_multiple_passes=False)

    def checkpoint(self):
        pass

    def mapPartitionsWithIndex(self, func):
        func = check_callable(func)

        def wrapper(source_seq):
            return check_iterable(func(0, source_seq))

        return self._chain(wrapper)

    def mapPartitions(self, func):
        return self._chain(check_callable(func))

    def map(self, func):
        return self._chain(partial(map, check_callable(func)))

    def filter(self, func):
        return self._chain(partial(filter, check_callable(func)))

    def flatMap(self, func):
        func = check_callable(func)

        def expander(source_seq):
            for x in check_iterable(source_seq):
                yield from check_iterable(func(x))

        return self._chain(expander)

    def mapValues(self, func):
        func = check_callable(func)

        def value_mapper(entry):
            k, v = unpack_key_value_pair(entry)
            return k, func(v)

        return self.map(value_mapper)

    def flatMapValues(self, func):
        func = check_callable(func)

        def wrapper(source_seq):
            for entry in check_iterable(source_seq):
                k, v = unpack_key_value_pair(entry)
                for v2 in check_iterable(func(v)):
                    yield k, v2

        return self._chain(wrapper)

    def union(self, other: 'MockRDD') -> 'MockRDDD':
        if not isinstance(other, MockRDD):
            raise TypeError(f"{other} isn't a MockRDD")
        return self.__class__(identity, chain(self, other), rnd=self.rnd,
                              allow_multiple_passes=self.allow_multiple_passes and other.allow_multiple_passes)

    def coalesce(self, numPartitions):
        check_partitions(numPartitions)
        return self

    def keyBy(self, func):
        func = check_callable(func)
        return self.map(lambda x: (func(x), x))

    def keys(self):
        def get_key(obj):
            k, v = unpack_key_value_pair(obj)
            return k

        return self.map(get_key)

    def values(self):
        def get_value(obj):
            k, v = unpack_key_value_pair(obj)
            return v

        return self.map(get_value)

    def partitionBy(self, numPartitions, partitionFunc=None):
        check_partitions(numPartitions)
        check_optional_callable(partitionFunc)
        return self

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners, numPartitions=None, partitionFunc=None):
        createCombiner = check_callable(createCombiner)
        mergeValue = check_callable(mergeValue)
        mergeCombiners = check_callable(mergeCombiners)
        check_partitions(numPartitions)
        check_optional_callable(partitionFunc)

        def wrapper(source_seq):
            return run_combine_by_key(source_seq, createCombiner, mergeValue, mergeCombiners, rnd=self.rnd)

        return self._chain(wrapper)

    def groupByKey(self, numPartitions=None, partitionFunc=None):
        check_partitions(numPartitions)
        check_optional_callable(partitionFunc)

        def create_combiner(v):
            return [v]

        def merge_value(v, x):
            v.append(x)
            return v

        def merge_combiners(a, b):
            a.extend(b)
            return a

        return self.combineByKey(create_combiner, merge_value, merge_combiners, numPartitions)

    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None, partitionFunc=None):
        seqFunc = check_callable(seqFunc)
        combFunc = check_callable(combFunc)
        check_partitions(numPartitions)
        check_optional_callable(partitionFunc)

        def createZero():
            return copy.deepcopy(zeroValue)

        return self.combineByKey(lambda v: seqFunc(createZero(), v), seqFunc, combFunc, numPartitions)

    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=None):
        func = check_callable(func)
        check_partitions(numPartitions)
        check_optional_callable(partitionFunc)

        def createZero():
            return copy.deepcopy(zeroValue)

        return self.combineByKey(lambda v: func(createZero(), v), func, func, numPartitions)

    def distinct(self, numPartitions=None, partitionFunc=None):
        check_partitions(numPartitions)
        check_optional_callable(partitionFunc)
        return self.mapPartitions(set)

    def reduceByKey(self, func, numPartitions=None, partitionFunc=None):
        check_partitions(numPartitions)
        check_optional_callable(partitionFunc)
        return self.combineByKey(identity, func, func, numPartitions)

    def reduce(self, func):
        return reduce(check_callable(func), self)

    def fold(self, zeroValue, func):
        return reduce(check_callable(func), self, zeroValue)

    def aggregate(self, zeroValue, seqOp, combOp):
        seqOp = check_callable(seqOp)
        combOp = check_callable(combOp)
        accs = []
        for c in chunk(self, 2):
            acc = zeroValue
            for obj in c:
                acc = seqOp(acc, obj)
            accs.append(acc)
        return reduce(combOp, accs)

    def cogroup(self, other: 'MockRDD', numPartitions=None):
        check_partitions(numPartitions)

        union = add_value_type(self, 0).union(add_value_type(other, 1))

        def run_cogroup(xs):
            entries = defaultdict(lambda: defaultdict(list))
            for k, (index, v) in xs:
                entries[k][index].append(v)
            for k, lists in entries.items():
                yield k, (lists[0], lists[1])

        return union._chain(run_cogroup)

    def intersection(self, other: 'MockRDD'):
        def make_kv(mock_rdd):
            return mock_rdd.map(lambda value: (value, None))

        def has_values_for_both(key_values):
            k, values = unpack_key_value_pair(key_values)
            return all(typing.cast(typing.Iterator, values))

        return make_kv(self).cogroup(make_kv(other)).filter(has_values_for_both).keys()

    def max(self, key=None):
        return max(self) if key is None else max(self, key)

    def min(self, key=None):
        return min(self) if key is None else min(self, key)

    def sum(self):
        return sum(self)

    def count(self):
        return sum(1 for _ in self)

    def countByValue(self):
        return Counter(self)

    def countByKey(self):
        return self.keys().countByValue()

    def take(self, num):
        return [x for _, x in zip(range(num), self)]

    def first(self):
        try:
            return next(iter(self))
        except StopIteration:
            raise ValueError("RDD is empty")
