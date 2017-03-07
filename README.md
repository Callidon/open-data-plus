# Capstone project Open Data Plus
Extraction techniques used to find content related to Nantes in Web Data Commons.

# Table of contents
* [Prerequisites](#prerequisites)
* [Installation](#installation)
* [Cluster setup](#cluster-setup)
  * [Configuration files](#configuration-files)
  * [Deployment](#deployment)
* [Usage](#usage)
* [License](#license)
* [Authors](#authors)

# Prerequisites
* [Scala](http://www.scala-lang.org/) version 2.11
* [Apache Hadoop](https://hadoop.apache.org/) version 2.6.5
* [Apache Spark](https://spark.apache.org/) version 2.0.1
* [Maven](https://maven.apache.org/)

# Installation

Clone the repository and then build it using Maven
```bash
git clone https://github.com/Callidon/open-data-plus.git
cd open-data-plus/
mvn package
```
# Usage
```
Open Data Plus Crawler: Analyze Web Data Commons using Apache Spark
Usage: <operation> <options>
Operations:
  -s1 <data-files>    Run stage 1 with the given data files
  -s2 <stage1-files> <data-files>    Run stage 2 using the output of stage 1 and the data files
  -agg <stage2-files> <output-directory>    Run aggregation using the output of stage 2 and store it in a directory
  -s3 <stage2-files> <output-directory>    Run stage 3 (aggregation + decision tree) using the output of stage 2 and store it in a directory
```

# Cluster setup

* Install Hadoop on every machine of the cluster (master + slaves)
* Install Apache Spark on the master
* Create/edit the following configuration files

## Configuration files

All configuration files must be placed in `$HADOOP_HOME/etc/hadoop`.

### Master

* `slaves`: This file lists the hosts, one per line. It should not contains the IP address of the master!

### Master and slaves

* `core-site.xml`: replace `$HOSTNAME` by the IP address of the current host (ex: 172.16.134.152), and `$HADOOP_DIR` by the location where files will be stored by the HDFS (make sure you have enough space!)
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://$HOSTNAME:9000</value>
  </property>
  <property>
  	<name>hadoop.tmp.dir</name>
  	<value>$HADOOP_DIR/tmp/hadoop-${user.name}</value>
  </property>
</configuration>
```

* `yarn-site.xml`: replace `$MASTER_HOST` by the IP address of the master (ex: 172.16.134.152)
```xml
<configuration>
	<property>
        <name>yarn.resourcemanager.hostname</name>
        <value>$MASTER_HOST</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
</configuration>
```

* `mapred-site.xml`: replace `$MASTER_HOST` by the IP address of the master (ex: 172.16.134.152)
```xml
<configuration>
	<property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
      <name>mapred.job.tracker</name>
      <value>$MASTER_HOST:9001</value>
   </property>
</configuration>
```

## Deployment

On the master, run the following scripts to start the cluster
```bash
# start master then slaves
$HADOOP_HOME/sbin/start-all.sh

# start YARN resource manager
$HADOOP_HOME/sbin/start-yarn.sh
```

To ensure that the cluster is running correctly, run the `jsp` command on each machine.

On the master, you should see :
* Namenode
* Resource Manager
* SecondaryNamenode
* JSP

On any slave, you should see :
* Datanode
* JSP

You can access Hadoop application's control panel at `http://localhost:50070`(on the master).

On the master, run the following scripts to shutdown the cluster
```bash
# stop master then slaves
$HADOOP_HOME/sbin/stop-all.sh

# stop YARN resource manager
$HADOOP_HOME/sbin/stop-yarn.sh
```

# Usage

Once the cluster has been deployed, you must upload all the files you want to evaluate on the Hadoop file system.

Then, you can launch the crawler using `spark-submit`

Example:
```
spark-submit --class com.alma.opendata.CLI target/open-data-crawler-1.0-SNAPSHOT-jar-with-dependencies.jar -s1 data/*.nq
```

You can see the progress of the task at `http://localhost:8080`(on the master). **This will also gives you the spark master url.**

## Useful `spark-submit` options :

* `--driver-memory <memory>` can be used to set how many memory the driver will use (in local mode)
* `--executor-memory <memory>` can be used to set how many memory slaves will use (by default, 1G).
* `--num-executors <number>` can be used to describes how many executors which will execute the application. We recommend to set this to the number of slaves in the cluster.

# License

[MIT License](https://github.com/Callidon/open-data-plus/blob/master/LICENSE)

# Authors

Adel Benabadji, Hiba Benyahia, Asma Boussalem, Théo Couraud, Pierre Gaultier, Lenny Lucas & Thomas Minier
