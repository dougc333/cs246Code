#!/bin/bash


#  sudo -u hdfs hadoop fs -rm -r -skipTrash /tmp/outputwordcount
# hadoop fs -copyFromLocal file:///home/dougc/cs246Code/hw11a/a.txt /tmp/testfile.txt

sudo -u hdfs hadoop jar target/hw11a-0.0.1-SNAPSHOT.jar cs246workinggroup.hw11a.WordCount
