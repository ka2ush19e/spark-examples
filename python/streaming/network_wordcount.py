# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

import sys

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.context import StreamingContext


if __name__ == '__main__':
    conf = SparkConf().setAppName('Network Word Count')
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]), StorageLevel.MEMORY_AND_DISK_SER)
    word_counts = (
        lines.flatMap(lambda l: l.split(' '))
        .map(lambda w: (w, 1L))
        .reduceByKey(lambda x, y: x + y)
    )

    word_counts.pprint()

    ssc.start()
    ssc.awaitTermination()
