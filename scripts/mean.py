#!/usr/bin/python
import fileinput as fi

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

list=[]
for line in fi.input():
	number=line.rstrip('\n')
	if is_number(number):
		list.append(float(number)) 

mean = float(sum(list))/len(list) if len(list) > 0 else float('nan')
print "{0:.3f}".format(mean)
