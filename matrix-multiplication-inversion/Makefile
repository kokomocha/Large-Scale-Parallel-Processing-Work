# Makefile for Spark WordCount project.

# Customize these paths for your environment.
# -----------------------------------------------------------
spark.root=/usr/local/spark-3.3.2-bin-without-hadoop#changed
hadoop.root=/usr/local/hadoop_3.3.5#changed
app.name=Matrix-Multiplication
jar.name=Matrix-Multiplication.jar
maven.jar.name=Matrix-Multiplication-1.0.jar
job.name=matmulDense.matmulDense
local.master=local[4]
local.input=input
local.output=output/check
param.size=3,3
local.logs=logs/local/spark
aws.local.output=output/aws/${exp.name}/${aws.cluster}
aws.local.logs=aws_logs/matmulDense/${exp.name}/${aws.cluster}

# Pseudo-Cluster Execution
hdfs.user.name=joe
hdfs.input=input
hdfs.output=output

# AWS EMR Execution
aws.input1=input/mat1/148x23000.csv
aws.input2=input/mat2/23000x148.csv
aws.input=input
aws.size1=148
aws.size2=23000
aws.output=output/matmul/${exp.name}

aws.emr.release=emr-6.10.0
aws.bucket.name=cs6240-pro

aws.log.dir=logs
aws.num.nodes=6
aws.instance.type=m4.xlarge
aws.cluster=j-2MCMFDV8VKQL4

exp.name=${aws.size1}-${aws.size2}-${aws.num.nodes}Worker
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package
	cp target/${maven.jar.name} ${jar.name}

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

clean-local-logs:
	rm -rf ${local.logs}*
clean-aws-local-output:
	rm -rf ${aws.local.output}*

clean-aws-local-logs:
	rm -rf ${aws.local.logs}*

# Runs standalone
local: jar clean-local-output
	spark-submit --class ${job.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input}/mat1/mat1.csv ${local.input}/mat2/mat2.csv ${local.output} ${param.size}
	# spark-submit --class ${job.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output} # to run linear regression, make sure the inputs are formatted correctly

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
download-output-hdfs:
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	spark-submit --class ${job.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	spark-submit --class ${job.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: #make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}/*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.name} s3://${aws.bucket.name}

# Main EMR launch.
aws-matmul: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Matrix-Multiplication ${exp.name} ${aws.num.nodes} Worker Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
		--applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input1}","s3://${aws.bucket.name}/${aws.input2}","s3://${aws.bucket.name}/${aws.output}/exp3","${aws.size1}","${aws.size2}","${aws.num.nodes}"] \
		# steps to run linearRegression
		# --steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input1}","s3://${aws.bucket.name}/${aws.input2}","s3://${aws.bucket.name}/${aws.input3}","s3://${aws.bucket.name}/${aws.input4}","s3://${aws.bucket.name}/${aws.output}"] \
          		#        		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--configurations '[{"Classification": "hadoop-env", "Configurations": [{"Classification": "export","Configurations": [],"Properties": {"JAVA_HOME": "/usr/lib/jvm/java-11-amazon-corretto.x86_64"}}],"Properties": {}}, {"Classification": "spark-env", "Configurations": [{"Classification": "export","Configurations": [],"Properties": {"JAVA_HOME": "/usr/lib/jvm/java-11-amazon-corretto.x86_64"}}],"Properties": {}}]' \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate
		
# Download output from S3.
download-output-aws: clean-aws-local-output
	mkdir ${aws.local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${aws.local.output}

# Download logs from s3
download-logs-aws: clean-aws-local-logs
	mkdir -p ${aws.local.logs}
	aws s3 sync s3://${aws.bucket.name}/${aws.log.dir}/${aws.cluster} ${aws.local.logs}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f Spark-Demo.tar.gz
	rm -f Spark-Demo.zip
	rm -rf build
	mkdir -p build/deliv/Spark-Demo
	cp -r src build/deliv/Spark-Demo
	cp -r config build/deliv/Spark-Demo
	cp -r input build/deliv/Spark-Demo
	cp pom.xml build/deliv/Spark-Demo
	cp Makefile build/deliv/Spark-Demo
	cp README.txt build/deliv/Spark-Demo
	tar -czf Spark-Demo.tar.gz -C build/deliv Spark-Demo
	cd build/deliv && zip -rq ../../Spark-Demo.zip Spark-Demo
	
