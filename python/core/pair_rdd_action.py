# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName('Pair RDD Action')
    sc = SparkContext(conf=conf)

    pairs = sc.parallelize([(1, "a"), (1, "b"), (1, "c"), (2, "d"), (2, "e")]).persist()

    print('### keys ###')
    print(pairs.keys().collect())
    print()

    print('### values ###')
    print(pairs.values().collect())
    print()

    print('### countByKey ###')
    for (n, c) in sorted(pairs.countByKey().items()):
        print('{}: {}'.format(n, c))
    print()

    print('### collectAsMap ###')
    for (n, c) in sorted(pairs.collectAsMap().items()):
        print('{}: {}'.format(n, c))
    print()

    print('### lookup ###')
    print("1: {}".format(pairs.lookup(1)))
    print("5: {}".format(pairs.lookup(5)))
    print()

    sc.stop()
