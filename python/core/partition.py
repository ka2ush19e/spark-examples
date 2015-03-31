# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals
import random

from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName('Partition')
    sc = SparkContext(conf=conf)

    def hash_partitioner(n):
        return hash(n)

    def random_partitioner(n):
        return random.randint(0, 10)

    nums1 = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')], 2).persist()
    nums2 = nums1.partitionBy(2, hash_partitioner)
    nums3 = nums1.partitionBy(2, random_partitioner)

    print('### Default ###')
    print('partition size: {}'.format(nums1.getNumPartitions()))
    nums1.foreachPartition(lambda p: print([e for e in p]))
    print()

    print('### HashPartitioner ###')
    print('partition size: {}'.format(nums2.getNumPartitions()))
    nums2.foreachPartition(lambda p: print([e for e in p]))
    print()

    print('### RandomPartitioner ###')
    print('partition size: {}'.format(nums3.getNumPartitions()))
    nums3.foreachPartition(lambda p: print([e for e in p]))
    print()

    sc.stop()
