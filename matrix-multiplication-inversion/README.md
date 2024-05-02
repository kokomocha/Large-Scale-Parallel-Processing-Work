# CS6240 Project Final Submission
The following github repository is the submission for the group consisting of Komal Pardeshi and Sean Yu. The repository is largely adapted from the starter scala spark repository from the assignments. The execution section  several adaptations and specific instructions to run the code.


Author
-----------
- Joe Sackett (2018)
- Updated by Nikos Tziavelis (2023)
- Updated by Mirek Riedewald (2024)
- Updated by Komal Pardeshi and Sean Yu (2024)

Installation
------------
These components need to be installed first:
- OpenJDK 11
- Hadoop 3.3.5
- Maven (Tested with version 3.6.3)
- AWS CLI (Tested with version 1.22.34)

- Scala 2.12.17 (you can install this specific version with the Coursier CLI tool which also needs to be installed)
- Spark 3.3.2 (without bundled Hadoop)

After downloading the hadoop and spark installations, move them to an appropriate directory:

`mv hadoop-3.3.5 /usr/local/hadoop-3.3.5`

`mv spark-3.3.2-bin-without-hadoop /usr/local/spark-3.3.2-bin-without-hadoop`

Environment
-----------
1) Example ~/.bash_aliases:
	```
	export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
	export HADOOP_HOME=/usr/local/hadoop-3.3.5
	export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
	export SCALA_HOME=/usr/share/scala
	export SPARK_HOME=/usr/local/spark-3.3.2-bin-without-hadoop
	export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin
	export SPARK_DIST_CLASSPATH=$(hadoop classpath)
	```

2) Explicitly set `JAVA_HOME` in `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:

	`export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`

Inputs
---------
The data to run the experiments can be found in the following OneDrive folder:
https://northeastern-my.sharepoint.com/:f:/g/personal/yu_sea_northeastern_edu/EuaqPtrv3_tIgLCFd6aNUDoBMId3wMIE_k5u3T5q10Auiw?e=zd84c8
To run the code locally add the input file to the inputs folder and update the local filepath in the makefile.

Execution
---------
<ins>Matrix Multiplcation - H-V </ins>
To run this program, change the input program in the makefile to: matmulGeneric.matmul

Params aws.bucket.name, aws.log.dir, aws.instance.type, aws.num.nodes may be changed as per requirements.

Change the aws params under change for every execution-> 
aws.experiment_id, aws.input1, aws.input2, aws.input, aws.output, aws.size1, aws.size2, aws.cluster.

<ins>Matrix Multiplcation - V-H adapted for A$^T$A</ins>
To run this program change the input program in the makefile to: ATAMatMulMain

<ins>Matrix Inversion</ins>
To run this program change the input program in the makefile to: matrixInversion.matrixInversionMain

<ins>Linear Regression</ins>
There are four inputs required to run this program. Namely, the train feature matrix, test feature matrix, train values vector, and test values vector. When running locally the program takes all the inputs as a single string separated by a comma. 
For example, in the makefile the line for local input should look like the following:
`local.input=input/trainA.csv,input/testA.csv,input/trainb.csv,input/testb.csv`

To run on AWS, the input arguments are separated. In addition to the bucket name being correct, the aws inputs should look like the following:

`aws.input1=normTrainA.csv
aws.input2=normTestA.csv
aws.input3=normTrainb.csv
aws.input4=normTestb.csv`

Lastly, several lines in the program need to be commented out/in in the file `src\main\scala\linearRegression\linearRegressionFinal.scala` depending if the code is to run locally or on aws.

To run locally, lines 161-165 should be kept and lines 168-171 should be commented out.
Conversly, to run on AWS lines 168-171 should be kept, whereas lines 161-165 should be commented out.

All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	- `make switch-standalone`		-- set standalone Hadoop environment (execute once)
	- `make local`
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	- `make switch-pseudo`			-- set pseudo-clustered Hadoop environment (execute once)
	- `make pseudo`					-- first execution
	- `make pseudoq`				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	- `make make-bucket`			-- only before first execution
	- `make upload-input-aws`		-- only before first execution
	- `make aws`					-- check for successful execution with web interface (aws.amazon.com)
	- `download-output-aws`		-- after successful execution & termination
	
	
Additional Edits
----------------
The make file was edited to allow the program to be run locally and on aws. Similarly, the spark master was changed on line 16-17 of WordCount.scala to allow the program to run locally and on aws.
