# ccms-log-parser

# Building ccms-log-parser

1. Run ./build.sh

The output ccmslogparser will be copied in the build/bin directory.

# Usage

./ccmslogparser 
-t <arg> Number of threads
-c <arg> Chunk size in bytes to read, per thread
-o <arg> Output path of csv file
-d If specified it will print additional debug information



Example :

./ccmslogparser -t 4 -c 1048576 -o ./ccms_MANUAL_2021-05-06_23-02-11-264_1.csv ./ccms_MANUAL_2021-05-06_23-02-11-264_1.log -d

