#!/usr/bin/python

from fabricate import *

programs = ['create', 'read', 'update', 'delete', 'filter', 'nearest']

def build():
	for program in programs:
		sources = [program, 'clustergis']
		compile(sources)
		link(sources, program)

def compile(sources):
	for source in sources:
		run('mpicc -Wall -O3 `geos-config --cflags` -c ' + source + '.c')

def link(sources, program='a.out'):
	objects = ' '.join(s + '.o' for s in sources)
	run('mpicc -o ' + program + ' -Wall -O3 `geos-config --cflags` ' + objects + ' `geos-config --ldflags` -lgeos_c')

def clean():
	autoclean()

main()
