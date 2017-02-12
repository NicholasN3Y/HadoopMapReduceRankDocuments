#!/bin/bash

cd /home/mininet/workspace/RankTopK/src/assgn1/cs4225
echo -e  "\n########## compiling java code ##########\n"
javac -cp `hadoop classpath` RankTopK.java -d ~/workspace/RankTopK/bin
echo -e  "\n########## java classes compiled ##########\n"
cd /home/mininet/workspace/RankTopK/bin
echo -e  "\n########## creating jar file ##########\n"
jar -cvf RankTopK.jar .
echo -e  "\n########## clean previous hadoop Job output directory ##########\n"
hadoop fs -rm -R /assignment_0/output
hadoop fs -rm -R /assignment_0/intermediate_result

echo -e  "\n########## start hadoop MapReduce Job ##########\n"
hadoop jar RankTopK.jar assgn1.cs4225.RankTopK /assignment_0/input /assignment_0/output /assignment_0/exclusions/sw4.txt /assignment_0/search/query.txt
echo -e  "\n########## get output results from hadoop fs ##########\n"
rm ../part-r-00000
hadoop fs -get /assignment_0/output/part-r-00000 ../
rm ../1.txt
hadoop fs -get /assignment_0/intermediate_result/1/part-r-00000 ../1.txt
rm ../2.txt
hadoop fs -get /assignment_0/intermediate_result/2/part-r-00000 ../2.txt
rm ../3.txt
hadoop fs -get /assignment_0/intermediate_result/3/part-r-00000 ../3.txt

cd ..
nano part-r-00000
echo -e  "\n########## DONE! ##########\n"


