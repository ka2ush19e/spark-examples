# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, division, unicode_literals

import re
import sys

from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName('Word Count')
    sc = SparkContext(conf=conf)

    stop_words = sc.broadcast(set([w.strip() for w in open(sys.argv[2]).readlines()]))
    stop_word_count = sc.accumulator(0)

    def filter_stop_words(w):
        global stop_word_count
        if w in stop_words.value:
            stop_word_count += 1
            return False
        return True

    word_counts = (
        sc.textFile(sys.argv[1])
        .flatMap(lambda l: re.split('[\s,.:;\'\"?!\\-\\(\\)\\[\\]\\{\\}]', l))
        .map(lambda w: w.lower())
        .filter(lambda w: len(w) > 1)
        .filter(filter_stop_words)
        .map(lambda w: (w, 1L))
        .reduceByKey(lambda x, y: x + y)
        .persist()
    )

    count = word_counts.map(lambda (w, c): c).sum()

    print('### All words count ###')
    print(count)
    print()

    print('### Stopped word count ###')
    print(stop_word_count.value)
    print()

    print('### Ranking ###')
    ranks = word_counts.sortBy(lambda (w, c): c, ascending=False).take(10)
    for (w, c) in ranks:
        print(w, c)
    print()

    print('### Ranking and rate ###')
    rank_rates = word_counts \
        .mapValues(lambda v: (v, v * 1.0 / count * 100)) \
        .sortBy(lambda (w, (c, r)): c, ascending=False) \
        .take(10)
    for (w, (c, r)) in rank_rates:
        print(w, c, r)
    print()

    sc.stop()
