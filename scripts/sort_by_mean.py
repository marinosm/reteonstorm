#!/usr/bin/python
import fileinput

data_list = [line for line in fileinput.input()]

data_list.sort(key= lambda line: float(line.split(" ")[0]))

for line in data_list:
	print line,

