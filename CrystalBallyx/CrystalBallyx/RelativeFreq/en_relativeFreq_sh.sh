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
 jar -cvf RelativeFreq.jar -C bin/ .

# hadoop fs -mkdir /user/cloudera/

 hadoop fs -test -d /user/cloudera/input
 if [ $? -ne 0 ]; then
   hadoop fs -mkdir /user/cloudera/input
 fi

#copy input file to HDFS
 hadoop fs -test -e /user/cloudera/input/retail.bat
 if [ $? -ne 0 ]; then
  echo "not exists, put data file to input"
  hadoop fs -put retail.bat /user/cloudera/input
 fi

 hadoop fs -test -d /user/cloudera/PairCoocurOutput
 if [ $? -eq 0 ]; then
   echo "Pair output exist and removed"
   hadoop fs -rm -r -f /user/cloudera/PairCoocurOutput
 fi

 hadoop fs -test -d /user/cloudera/StripeOutput
 if [ $? -eq 0 ]; then
   echo "Pair output exist and removed"
   hadoop fs -rm -r -f /user/cloudera/StripeOutput
 fi

 hadoop fs -test -d /user/cloudera/HybridOutput
 if [ $? -eq 0 ]; then
   echo "Pair output exist and removed"
   hadoop fs -rm -r -f /user/cloudera/HybridOutput
 fi


#run jar on hadoop hdfs
#hadoop jar p.jar part2.Pairs /user/cloudera/pair/input /user/cloudera/pair/output
#hdfs dfs -rm -f -r /user/cloudera/PairCoocurOutput
hadoop jar RelativeFreq.jar com.relativefreq.main.Main PairCoocur 4 /user/cloudera/input/test.txt /user/cloudera/PairCoocurOutput
#hdfs dfs -rm -f -r /user/cloudera/StripeOutput
hadoop jar RelativeFreq.jar com.relativefreq.main.Main Stripe 4 /user/cloudera/input/test.txt /user/cloudera/StripeOutput
#hdfs dfs -rm -f -r /user/cloudera/HybridOutput
hadoop jar RelativeFreq.jar com.relativefreq.main.Main Hybrid 4 /user/cloudera/input/test.txt /user/cloudera/HybridOutput

#to see the output of the HDFS
# hadoop fs -cat /user/cloudera/pair/output/*
