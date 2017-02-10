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
echo -e  "\n########## start hadoop MapReduce Job ##########\n"
hadoop jar RankTopK.jar assgn1.cs4225.RankTopK /assignment_0/exclusions/sw4.txt /assignment_0/input /assignment_0/output
echo -e  "\n########## get output results from hadoop fs ##########\n"
rm ../part-r-00000
hadoop fs -get /assignment_0/output/part-r-00000 ../
cd ..
nano part-r-00000
echo -e  "\n########## DONE! ##########\n"


