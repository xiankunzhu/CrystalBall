#!/bin/sh
#
# Script to  Pair Approuch
#
echo starting ...
# sudo su hdfs

#if we need to compile it by batch file  run below code
# javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* JavaFiles/*.java  -d ClassFiles -Xlint

#else just compile the .class to the jar file
#  jar -cvf p.jar -C ClassFiles/ .

# packege the .class file to jar file
 jar -cvf WordCount.jar -C bin/ .

# hadoop fs -mkdir /user/cloudera/

 hadoop fs -test -d /user/cloudera/input
 if [ $? -ne 0 ]; then
   hadoop fs -mkdir /user/cloudera/input
 fi

#copy input file to HDFS
 hadoop fs -test -e /user/cloudera/input/sample.txt
 if [ $? -ne 0 ]; then
  echo "not exists, put data file to input"
  hadoop fs -put sample.txt /user/cloudera/input
 fi

 hadoop fs -test -d /user/cloudera/output
 if [ $? -eq 0 ]; then
   echo "Pair output exist and removed"
   hadoop fs -rm -r -f /user/cloudera/output
 fi

#run jar on hadoop hdfs
hadoop jar WordCount.jar WordCount /user/cloudera/input/sample.txt /user/cloudera/output

#to see the output of the HDFS
# hadoop fs -cat /user/cloudera/pair/output/*
