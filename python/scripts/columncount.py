#!/usr/bin/python

import sys

inputs = sys.stdin.readlines()
for input in inputs:
    columns = input.strip().split(',')
    print len(columns)

