rm -rf outputWC
call hadoop jar c:\hadoop-2.8.2\share\hadoop\mapreduce\hadoop-mapreduce-examples-2.8.2.jar wordcount input outputWC
rm -rf output2
call hadoop jar c:\hadoop-2.8.2\share\hadoop\mapreduce\hadoop-mapreduce-examples-2.8.2.jar grep input output2 prop
