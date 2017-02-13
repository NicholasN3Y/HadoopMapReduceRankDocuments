Author: Nicholas Ng Nuo Yang
Matric: A0112224B

** MUST READ! **

assignment 1 of CS4225

What this program is about:
A MapReduce program for hadoop that ranks Document's with respect to a list of query terms.

sw-file-path      - path to stop words file.
query-file-path   - text file of query terms.
source-path       - path to folder directory of input files
dest-path         - path to folder directory of output
** all the paths above are wrt to the hadoop file system. **

*************
bash script -> run.sh contains a list of commands that automates the process of running in hadoop

execute the run.sh as follows:
<> denotes arguments that are compulsory
() denotes arguments that are optional

./run.sh <source-path> <dest-path> <sw-file-path> <query-file-path> [K]

eg. 
./run.sh a0112224/assignment_0/input a0112224/assignment_0/output sw4.txt query.txt
./run.sh a0112224/assignment_0/input a0112224/assignment_0/output sw4.txt query.txt 10

the script runs the program in hadoop, prints the top K ranking in the console.
The output files from each MapReduce Job is transfered to local directory, and can be found in:
1.txt - Result for stage 1
2.txt - Result for stage 2
3.txt - Result for stage 3
4.txt - Result for stage 4
final_results.txt - Result for stage 5

*************


