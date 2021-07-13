bin_PROGRAMS = ccmslogparser ccmsloggenerate
ccmslogparser_SOURCES = main.cpp getopt.c
ccmsloggenerate_SOURCES = generate.cpp

AM_CPPFLAGS = -I../rapidcsv -std=c++11 -I../build/
AM_CFLAGS = -I../build/ -std=c11

all-local:
	@echo 'Copy output files'
	@mkdir -p ../bin/
	@cp ../src/ccmslogparser ../bin/ccmslogparser
