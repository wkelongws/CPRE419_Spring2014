First clean the output folder and any temp folder:
hadoop fs -rmr /scr/yuz1988/lab4

Then run my program:
hadoop jar /home/yuz1988/lab4/MySort.jar MySort /class/s14419x/lab4/gensort-out-250M  /scr/yuz1988/lab4/output  /scr/yuz1988/lab4/temp1  /scr/yuz1988/lab4/temp2/partlist

There are four arguments passed to my program. First is the input directory, second is the final output directory, third is the directory for the temp which contains the copy of the input files but the format is SequnceFile consisting of binary key/value pairs, forth argument is a path for a file (not a directory) for parition list, so the name of my partition list file is partlist. The partition list should contain 14 keys (one less than the number of reducers).
