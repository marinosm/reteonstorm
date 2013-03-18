#!/usr/bin/python
import fileinput as fi

list=[]
for line in fi.input():
	list.append(float(line.rstrip('\n')))

print float(sum(list))/len(list) if len(list) > 0 else float('nan')
