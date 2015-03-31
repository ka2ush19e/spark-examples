# -*- coding: utf-8 -*-

from __future__ import print_function, division, unicode_literals

from pyspark import SparkConf, SparkContext

from case import Str


if __name__ == '__main__':
    conf = SparkConf().setAppName('Basic RDD Action')
    sc = SparkContext(conf=conf)

    nums = sc.parallelize([1, 1, 1, 2, 3, 3, 4, 4, 4, 5, 5, 5]).persist()
    strs = sc.parallelize([Str('a'), Str('b'), Str('c'), Str('d'), Str('e'), ]).persist()

    print('### first ###')
    print(nums.first())
    print()

    print('### collect ###')
    print(nums.collect())
    print()

    print('### count ###')
    print(nums.count())
    print()

    print('### countByValue ###')
    for (n, c) in sorted(nums.countByValue().items()):
        print('{}: {}'.format(n, c))
    print()

    print('### take ###')
    print(nums.take(5))
    print()

    print('### top ###')
    print(nums.top(4))
    print([s.s for s in strs.top(4, lambda x: x.s)])
    print()

    print('### takeOrdered ###')
    print(nums.takeOrdered(5))
    print([s.s for s in strs.takeOrdered(5, lambda x: x.s)])
    print()

    print('### takeSample ###')
    print(nums.takeSample(False, 3))
    print()

    print('### reduce ###')
    print(nums.reduce(lambda x, y: x + y))
    print()

    print('### fold ###')
    print(nums.fold(0, lambda x, y: x + y))
    print()

    print('### aggregate ###')
    print(nums.aggregate(0, lambda x, y: x + y, lambda x, y: x + y))
    print()

    print('### foreach ###')
    nums.foreach(lambda n: print(n))
    print()

    print('### foreachPartition ###')
    nums.foreachPartition(lambda p: print([n for n in p]))
    print()

    sc.stop()