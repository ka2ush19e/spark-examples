# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

import sys

from pyspark import SparkConf, SparkContext
from pyspark.streaming.context import StreamingContext


if __name__ == '__main__':
    conf = SparkConf().setAppName('Stateful Network Word Count')
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("checkpoint")

    def update(values, state):
        return (state or 0) + sum(values)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    word_counts = (
        lines.flatMap(lambda l: l.split(' '))
        .map(lambda w: (w, 1L))
        .updateStateByKey(update)
    )

    word_counts.pprint()

    ssc.start()
    ssc.awaitTermination()
