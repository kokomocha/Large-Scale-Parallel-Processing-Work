# Makefile for Hadoop MapReduce WordCount demo project.

#added local.logs, added commands to download aws logs[line137]
#changed bucketName,numNodes,InstanceType, cluster.id

# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/usr/local/hadoop_3.3.5
#jar Names
jar.name.rep=repJoin
jar.name.rs=rsJoin
#target Paths
jar.path.rep=target/${jar.name.rep}
jar.path.rs=target/${jar.name.rs}
#job Names
job.name.rs = TriangleCount.rsTriangleCountApprox
job.name.rep = TriangleCount.repTriangleApprox
#local folders
folder.rs=reduceJoin
folder.rep=replicatedJoin
local.input=input
local.output=output
local.logs=logs
# Pseudo-Cluster Execution
hdfs.user.name=joe
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-6.10.0
aws.region=us-east-1
aws.bucket.name=cs6240-hw2#
aws.subnet.id=subnet-013a8e010c9f73d03#
aws.input=input
aws.output=output
aws.log.dir=log
aws.num.nodes=8#
aws.instance.type=m6a.xlarge#
aws.cluster.id=j-2N11TIDS8KLCP#
# -----------------------------------------------------------
# Compiles code and builds 	(with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output-rs:
	cd ${local.output}
	rm -rf ${folder.rs}*

clean-local-output-rep:
	cd ${local.output}
	rm -rf ${folder.rep}*

clean-local-logs-rs:
	cd ${local.logs}
	rm -rf ${folder.rs}*

clean-local-logs-rep:
	cd ${local.logs}
	rm -rf ${folder.rep}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
local-rs: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path.rs} ${job.name.rs} ${local.input} ${local.output}/${folder.rs}

local-rep: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path.rep} ${job.name.rep} ${local.input} ${local.output}/${folder.rep}


##### ~ Not Configured PseudoMode ~ ##### ~ Do Not Use ~ #####
################################################################################################################
# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs: clean-local-output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output-hdfs

##################################################################################################################
# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws-rs:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}/${folder.rs}*"

delete-output-aws-rep:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}/${folder.rep}*"

# Upload application to S3 bucket.
upload-app-aws-rs:
	aws s3 cp ${jar.path.rs} s3://${aws.bucket.name}

upload-app-aws-rep:
	aws s3 cp ${jar.path.rep} s3://${aws.bucket.name}

# Main EMR launch.
aws-rs: jar upload-app-aws-rs delete-output-aws-rs
	aws emr create-cluster \
		--name "RS Join" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
		--applications Name=Hadoop \
		--steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}/${folder.rs}/intermediate1","s3://${aws.bucket.name}/${aws.output}/${jar.name.rs}/final"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name.rs}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir}/${folder.rs} \
		--configurations '[{"Classification": "hadoop-env", "Configurations": [{"Classification": "export","Configurations": [],"Properties": {"JAVA_HOME": "/usr/lib/jvm/java-11-amazon-corretto.x86_64"}}],"Properties": {}}]' \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

aws-rep: jar upload-app-aws-rep delete-output-aws-rep
	aws emr create-cluster \
		--name "Rep Join" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
		--applications Name=Hadoop \
		--steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}/${folder.rep}/final"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name.rep}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir}/${folder.rep} \
		--configurations '[{"Classification": "hadoop-env", "Configurations": [{"Classification": "export","Configurations": [],"Properties": {"JAVA_HOME": "/usr/lib/jvm/java-11-amazon-corretto.x86_64"}}],"Properties": {}}]' \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

# Download output from S3.
download-output-aws-rs: clean-local-output
	mkdir ${folder.rs}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}/${folder.rs}

download-output-aws-rep: clean-local-output
	mkdir ${folder.rep}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}/${folder.rep}

## Added functionality: Download logs from S3.
download-logs-aws-rs: clean-local-logs-rs
	mkdir ${folder.rs}
	aws s3 sync s3://${aws.bucket.name}/${aws.log.dir}/${aws.cluster.id} ${local.logs}/${folder.rs}

download-logs-aws-rep: clean-local-logs-rep
	mkdir ${folder.rep}
	aws s3 sync s3://${aws.bucket.name}/${aws.log.dir}/${aws.cluster.id} ${local.logs}/${folder.rep}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f twitter-Social-Triangles.tar.gz
	rm -f twitter-Social-Triangles.zip
	rm -rf build
	mkdir -p build/deliv/twitter-Social-Triangles
	cp -r src build/deliv/twitter-Social-Triangles
	cp -r config build/deliv/twitter-Social-Triangles
	cp -r input build/deliv/twitter-Social-Triangles
	cp pom.xml build/deliv/twitter-Social-Triangles
	cp Makefile build/deliv/twitter-Social-Triangles
	cp README.txt build/deliv/twitter-Social-Triangles
	tar -czf twitter-Social-Triangles.tar.gz -C build/deliv twitter-Social-Triangles
	cd build/deliv && zip -rq ../../twitter-Social-Triangles.zip twitter-Social-Triangles
