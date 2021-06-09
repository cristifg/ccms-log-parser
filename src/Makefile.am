bin_PROGRAMS = ccmslogparser ccmsloggenerate
ccmslogparser_SOURCES = main.cpp
ccmsloggenerate_SOURCES = generate.cpp

AM_CPPFLAGS = -I../rapidcsv -std=c++11 -I../build/

all-local:
	@rm -rf ./build
	@echo 'Copy output files'
	@mkdir -p ../bin/
	@cp -f ../src/ccmslogparser ../bin/ccmslogparser
