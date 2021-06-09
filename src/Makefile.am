bin_PROGRAMS = ccmslogparser ccmsloggenerate
ccmslogparser_SOURCES = main.cpp
ccmsloggenerate_SOURCES = generate.cpp

AM_CPPFLAGS = -I../rapidcsv -std=c++11 -I../build/

all-local:
	@echo 'Copy output files'
	@mkdir -p ../bin/
	@cp ../src/ccmslogparser ../bin/ccmslogparser
