#!/usr/bin/python
from sys import argv
from scipy import stats
from numpy import array

# given two or more files

argv.pop(0) #remove the script name arg
samples=[]
for fname in argv:
	 samples.append(array([float(line.strip()) for line in open(fname)]))

f_val, p_val = stats.f_oneway(*samples)
print p_val
