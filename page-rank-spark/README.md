<p align="center">  
    <br>
	<a href="#">
        <img height=100 src="https://cdn.svgporn.com/logos/scala.svg" alt="Scala" title="Scala" hspace=20 />
        <img height=100 src="https://cdn.svgporn.com/logos/maven.svg" alt="Maven" title="Maven" hspace=20 />
        <img height=100 src="https://cdn.svgporn.com/logos/apache-spark.svg" alt="Spark" title="Spark" hspace=20 /> 
  </a>	
</p>
<br>

# Hadoop MapReduce RS-Join and Replicated Join Demo
(Coded for Homework-3 Assignment)

## Author
- Komal Pardeshi

## Build Tested on
- Windows 10 with Oracle Virtualbox-7.0.14 using IntelliJ
(All the components under Installation section need to be installed on WSL.)

- Note: 
	1) IntelliJ has easier debugging tools and documentation for Scala than VSCode Metals for Scala.
	2) IntelliJ remote host(WSL) times out during long jobs and hence, would recommend using Virtualbox/VMware.

## Installation
### Optional:
Download Linux Distribution ISO Image for your Operating System online.(LTS Recommended) [Click here](https://releases.ubuntu.com/)

Install the Oracle Virtualbox from offical site. [Click here](https://www.virtualbox.org/wiki/Downloads)
1) Create New Instance by clicking on "New".
2) **Name and OS Tab:** Specify name, root location, and your Operating System specifications for VM.
- Add Username and Password(available on first virtual machine creation) to gain root access as required for operations.
 OR
 Default username and password(Unattended Install second instance onwards) -> password: "changeme" which could be edited later.
3) **Hardware Tab:** Specify Base Memory->2048 MB and Processors->5 or as per your usage(can be changed later if required).
4) **Hard Disk Tab:** Virtual Hard Disk->50 MB or as per your usage(can be changed later if required).
5) Click Finsh and Log in on start-up

(All the components below this need to be installed on WSL.)
These components need to be installed first:
- OpenJDK 11
- Hadoop 3.3.5
- Maven (Tested with version 3.6.3)
- AWS CLI (Tested with version 1.22.34)

After downloading the hadoop installation, move it to an appropriate directory:
`mv hadoop-3.3.5 /usr/local/hadoop-3.3.5`

## Environment
1) Example ~/.bash_aliases:
	```
	export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
	export HADOOP_HOME=/usr/local/hadoop-3.3.5
	export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
	export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
	```

2) Explicitly set `JAVA_HOME` in `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:
	`export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`

## Execution
All of the build & execution commands are organized in the Makefile.
1) Open terminal.
2) Clone project from github (SSH recommended, HTTPS discontinued on Linux).
3) Navigate to directory where project files unzipped.
4) Advisable to work in a virtual env.
5) For aws, update the Credentials in credentials file under `~/.aws/`.
6) Edit the Makefile to customize the environment at the top.

	**on ever Program execution, to save every JAR**
	
	- Sufficient for standalone: hadoop.root, jar.name, local.input, local.logs
	Other defaults acceptable for running standalone.
	or 
	- For aws: aws.region, aws.bucket.name, aws.subnet.id,  cluse
7) Standalone Hadoop:
	- `make switch-standalone`		-- set standalone Hadoop environment (execute once)
	- `make local`
8)  **Not Configured for this App** 

	Pseudo-Distributed Hadoop:
	(https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	- `make switch-pseudo`			-- set pseudo-clustered Hadoop environment (execute once)
	- `make pseudo`					-- first execution
	- `make pseudoq`				-- later executions since namenode and datanode already running 
9) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	(Before Exectution)
	- `make make-bucket`			-- only before first execution
	- `make upload-input-aws`		-- only before first execution
	
	### a - Reduce-side Join Commands(Use After Execution)
	#### (aws.num.nodes, aws.instance.type)
	- `make aws-rs`					-- check for successful execution with web interface for rs join app(aws.amazon.com)

	#### (Update aws.cluster.id)
	- `download-output-aws-rs`		-- downloads rs join output after successful execution & termination
	- `download-logs-aws-rs`		-- downloads rs join logs for execution

	### b - Replicated Join Commands(Use After Execution)
	#### (Update aws.num.nodes, aws.instance.type)
	- `make aws-rep`				-- check for successful execution with web interface for rep join app(aws.amazon.com)

	#### (Update aws.cluster.id)
	- `download-output-aws-rep`		-- downloads rep join output after successful execution & termination
	- `download-logs-aws-rep`		-- downloads rep join logs for execution

 