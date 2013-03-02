#!/usr/bin/python
import sys

def foo(subj, n):
	for i in range(0,n):
		print 'subj='+subj, 'pred=foo', 'obj='+str(i)

letters = ['a','b','c','d']
for l in letters:
	foo(l, int(sys.argv[1]))
