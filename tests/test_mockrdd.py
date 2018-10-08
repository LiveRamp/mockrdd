import unittest
from collections import Counter
import typing

from mockrdd import MockRDD, identity


def check_collections_equivalent(a: typing.Collection, b: typing.Collection,
                                 allow_duplicates: bool = False,
                                 element_converter: typing.Callable = identity) -> typing.Tuple[str, list]:
    """

    :param a: one collection to compare
    :param b: other collection to compare
    :param allow_duplicates: allow collections to contain multiple elements
    :param element_converter: optional function to convert elements of collections to a different value
                              for comparison
    :return: (message, differences)
    """
    a = Counter(map(element_converter, a))
    b = Counter(map(element_converter, b))
    if not allow_duplicates:
        duplicates = []
        for name, counts in [['a', a], ['b', b]]:
            for key, count in counts.items():
                if count > 1:
                    duplicates.append([name, key, count])
        if duplicates:
            return 'Duplicate elements ', ['|'.join(map(str, dup)) for dup in duplicates]

    diffs = []
    for el in a | b:
        ac = a.get(el, 0)
        bc = b.get(el, 0)
        if ac != bc:
            'Inconsistent element frequencies', diffs.append(f'{el} a={ac} b={bc}')
    if diffs:
        return "Inconsistent element frequencies: ", diffs

    return 'Collections equivalent', []


def col_eq(a, b, allow_duplicates=False, element_converter=identity):
    message, differences = check_collections_equivalent(a, b, allow_duplicates, element_converter)
    if differences:
        raise AssertionError(message + ": " + ", ".join(differences))


class TestMockRDD(unittest.TestCase):
    def test_empty(self):
        rdd = MockRDD(identity, [])
        self.assertEqual(list(rdd), [])

    def test_single(self):
        rdd = MockRDD(identity, [1])
        self.assertEqual(list(rdd), [1])

    def test_multiple(self):
        x = [1, 3, '5', (), 102]
        rdd = MockRDD(identity, x)
        self.assertEqual(list(rdd), x)

    def test_collect(self):
        rdd = MockRDD(identity, [1])
        self.assertEqual(rdd.collect(), [1])

    def test_only_one_pass(self):
        rdd = MockRDD(identity, iter(range(5)))
        self.assertEqual(rdd.collect(), list(range(5)))
        with self.assertWarns(Warning):
            self.assertEqual(rdd.collect(), [])

    def test_persist(self):
        rdd = MockRDD(identity, iter(range(5))).persist()
        for _ in range(10):
            self.assertEqual(rdd.collect(), list(range(5)))

    def test_mapPartitionsWithIndex(self):
        x = [1, 2, 3]

        def func(index, seq):
            assert list(seq) == x
            yield 'value'

        rdd = MockRDD(identity, x).mapPartitionsWithIndex(func)
        self.assertEqual(list(rdd), ['value'])

    def test_mapPartitions(self):
        x = [3, 's', None]

        def func(seq):
            assert list(seq) == x
            yield 'result'

        rdd = MockRDD(identity, x).mapPartitions(func)
        self.assertEqual(list(rdd), ['result'])

    def test_map(self):
        x = [1, 2, 3, 5, 8]

        rdd = MockRDD(identity, x).map(lambda a: 2 * a)
        self.assertEqual(list(rdd), [2, 4, 6, 10, 16])

    def test_filter(self):
        x = [1, 2, 3, 4, 5]

        rdd = MockRDD(identity, x).filter(lambda a: a % 2 == 0)
        self.assertEqual(list(rdd), [2, 4])

    def test_flatMap(self):
        x = [1, 2, 3]

        def func(el):
            assert el in x
            for i in range(el):
                yield i

        rdd = MockRDD.from_seq(x).flatMap(func)
        self.assertEqual(list(rdd), [0, 0, 1, 0, 1, 2])

    def test_mapValues(self):
        x = [('a', 1), ('b', 3)]

        rdd = MockRDD.from_seq(x).mapValues(lambda a: a + 1)
        self.assertEqual(list(rdd), [('a', 2), ('b', 4)])

    def test_flatMapValues(self):
        x = [('a', 1), ('b', 3)]

        def func(el):
            self.assertIn(el, (1, 3))
            for i in range(1, el):
                yield i

        rdd = MockRDD.from_seq(x).flatMapValues(func)
        self.assertEqual(list(rdd), [('b', 1), ('b', 2)])

    def test_union(self):
        x = [1, 2]
        y = [7, 8]
        rdd = MockRDD.from_seq(x).union(MockRDD.from_seq(y))
        self.assertEqual(list(rdd), x + y)

    def test_keys(self):
        kvs = [(1, 2), (3, 4)]
        rdd = MockRDD.from_seq(kvs).keys()
        results = list(rdd)
        self.assertEqual(results, [1, 3])

    def test_values(self):
        kvs = [(1, 2), (3, 4)]
        rdd = MockRDD.from_seq(kvs).values()
        results = list(rdd)
        self.assertEqual(results, [2, 4])

    def test_keyBy(self):
        xs = [1, 2]
        rdd = MockRDD.from_seq(xs).keyBy(lambda x: x % 2)
        results = list(rdd)
        self.assertEqual(results, [(1, 1), (0, 2)])

    def test_combineByKey(self):
        kvs = [('a', 1), ('b', 3), ('a', 5), ('b', 3), ('a', 1)]

        # Want to ensure that both value and combiner merging is performed
        # It's possible to do everything with only value merging, but if we did
        # we'd miss bugs in the merge combiner
        used_merge_value = False
        used_merge_combiners = False

        def create_combiner(v):
            return {v}

        def merge_value(v, x):
            nonlocal used_merge_value
            used_merge_value = True
            v.add(x)
            return v

        def merge_combiners(a, b):
            nonlocal used_merge_combiners
            used_merge_combiners = True
            a.update(b)
            return a

        rdd = MockRDD.from_seq(kvs).combineByKey(create_combiner, merge_value, merge_combiners)
        results = list(rdd)
        self.assertEqual(len(results), 2)
        results = dict(results)
        self.assertEqual(results, {'a': {1, 5}, 'b': {3}})
        self.assertTrue(used_merge_value)
        self.assertTrue(used_merge_combiners)

    def test_groupByKey(self):
        kvs = [('a', 1), ('b', 3), ('a', 5), ('b', 3)]

        rdd = MockRDD.from_seq(kvs).groupByKey()
        results = list(rdd)
        self.assertEqual(len(results), 2)
        results = dict(results)
        self.assertEqual(set(results), {'a', 'b'})
        self.assertEqual(set(results['a']), {1, 5})
        self.assertEqual(results['b'], [3, 3])

    def test_distinct(self):
        xs = [1, 2, 5, 1, 6, 8, 2, 90]
        d = set(xs)
        rdd = MockRDD.from_seq(xs).distinct()
        results = list(rdd)
        self.assertEqual(len(results), len(d))
        self.assertEqual(set(results), d)

    def test_reduceByKey(self):
        kvs = [('a', 1), ('b', 7), ('a', 2)]
        rdd = MockRDD.from_seq(kvs).reduceByKey(lambda a, b: a + b)
        results = list(rdd)
        self.assertEqual(len(results), 2)
        results = dict(results)
        self.assertEqual(results, {'a': 3, 'b': 7})

    def test_reduce(self):
        x = [1, 2, 3, 5]
        result = MockRDD.from_seq(x).reduce(lambda a, b: a + b)
        self.assertEqual(result, sum(x))

    def test_fold(self):
        x = [1, 2, 3, 5]
        i = 7
        result = MockRDD.from_seq(x).fold(i, lambda a, b: a + b)
        self.assertEqual(result, i + sum(x))

    def test_aggregate(self):
        zeroValue = set()

        def seqO(c, x):
            c.add(x)
            return c

        did_combine = False

        def combOp(a, b):
            nonlocal did_combine
            did_combine = True
            a.update(b)
            return a

        x = [1, 2, 3, 5, 2, 8, 3]
        result = MockRDD.from_seq(x).aggregate(zeroValue, seqO, combOp)
        self.assertEqual(result, set(x))
        self.assertTrue(did_combine)

    def test_cogroup_empty(self):
        self.assertEqual(MockRDD.empty().cogroup(MockRDD.empty()).count(), 0)

    def test_cogroup_only_one(self):
        empty = MockRDD.empty().persist()
        one = MockRDD.of(('k', 1)).persist()
        self.assertEqual(empty.cogroup(one).collect(), [('k', ([], [1]))])
        self.assertEqual(one.cogroup(empty).collect(), [('k', ([1], []))])

    def test_cogroup_match(self):
        zero = MockRDD.of(('k', 0)).persist()
        one = MockRDD.of(('k', 1)).persist()
        self.assertEqual(zero.cogroup(one).collect(), [('k', ([0], [1]))])
        self.assertEqual(one.cogroup(zero).collect(), [('k', ([1], [0]))])

    def test_cogroup_multiple_keys(self):
        a = MockRDD.of(('k1', 0), ('k2', 2)).persist()
        b = MockRDD.of(('k1', 1), ('k3', 3)).persist()

        def convert(x):
            k, (i0, i1) = x
            return k, (tuple(i0), tuple(i1))

        col_eq(a.cogroup(b).collect(), [('k1', ([0], [1])),
                                        ('k2', ([2], [])),
                                        ('k3', ([], [3]))], element_converter=convert)
        col_eq(b.cogroup(a).collect(), [('k1', ([1], [0])),
                                        ('k2', ([], [2])),
                                        ('k3', ([3], []))], element_converter=convert)

    def test_max(self):
        x = [1, 2, 3, 5, 2, 8, 3]
        self.assertEqual(MockRDD.from_seq(x).max(), 8)

    def test_min(self):
        x = [1, 2, 3, 5, 2, 8, 3]
        self.assertEqual(MockRDD.from_seq(x).min(), 1)

    def test_sum(self):
        x = [1, 5, 2]
        self.assertEqual(MockRDD.from_seq(x).sum(), 8)

    def test_count(self):
        x = [1, 2, 1]
        self.assertEqual(MockRDD.from_seq(x).count(), 3)

    def test_countByValue(self):
        x = [1, 3, 1]
        self.assertEqual(MockRDD.from_seq(x).countByValue(), {1: 2, 3: 1})

    def test_countByKey(self):
        x = [(1, 'a'), (3, 'a'), (1, 'b')]
        self.assertEqual(MockRDD.from_seq(x).countByKey(), {1: 2, 3: 1})


if __name__ == '__main__':
    unittest.main()
