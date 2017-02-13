#!/bin/bash

source=$1
dest=$2
sw=$3
query=$4
K=$5

if [[ $# -lt 4 ]] ; then
    echo "Usage run.sh <source-path> <dest-path> <stop-words-file-path> <query-file-path> [K]"
    exit 1
fi

cd src/assgn1/cs4225
echo -e  "\n########## compiling java code ##########\n"
javac -cp `hadoop classpath` RankTopK.java -d ../../../bin
echo -e  "\n########## java classes compiled ##########\n"
cd ../../../bin
echo -e  "\n########## creating jar file ##########\n"
jar -cvf RankTopK.jar .
echo -e  "\n########## clean previous hadoop Job output directory ##########\n"
hadoop fs -rm -R $dest
hadoop fs -rm -R a0112224/assignment_0/intermediate_result

echo -e  "\n########## start hadoop MapReduce Job ##########\n"
hadoop jar RankTopK.jar assgn1.cs4225.RankTopK $source $dest $sw $query $K
echo -e  "\n########## get output results from hadoop fs ##########\n"
rm ../final_results.txt
hadoop fs -get $dest/part-r-00000 ../final_results.txt
rm ../1.txt
hadoop fs -get a0112224/assignment_0/intermediate_result/1/part-r-00000 ../1.txt
rm ../2.txt
hadoop fs -get a0112224/assignment_0/intermediate_result/2/part-r-00000 ../2.txt
rm ../3.txt
hadoop fs -get a0112224/assignment_0/intermediate_result/3/part-r-00000 ../3.txt
rm ../4.txt
hadoop fs -get a0112224/assignment_0/intermediate_result/4/part-r-00000 ../4.txt

cd ..
echo -e "\nIntermediate results for job 1 to 4 can be found in txt files 1 - 4 respectively"
echo -e "\nSorted final results can be found in final_results.txt"
echo -e  "\n########## DONE! ##########\n"


