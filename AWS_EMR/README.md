AWS EMR

# overview

Elastic MapReduce is a managed clustered platform to run big data frameworks. 

![supported applications](./imgs/emr_services.png)

EMR is effective for OLAP (online analytical processing) and batch processing jobs. 
- at least TB worth of data processing
- suitable for "embarrassingly parallel" tasks, i.e.,  computational problems that can be easily divided into independent subtasks requiring little to no communication between them.
- By default, EMR uses YARN (yet another resource negotiator) for cluster resource management, with resource manager and node manager. 

![YARN Architecture](./imgs/yarn.png)  
*<center>YARN was introduced in Apache Hadoop 2.0</center>*

# Architecture for Amazon EMR

There are 3 node types

- primary node: coordinates distribution of data and tasks. Tracks status of tasks and monitors health of cluster.
- core node: Runs tasks and stores data in HDFS (Hadoop Distributed File System). In HDFS, data is stored across multiple instances, with multiple copies. Storage is ephemeral.
- task node: (optional) Runs tasks but does not store data.

![EMR Architecture](./imgs/emr_arch.png)

EMR clusters are Elastic
- Horizontal scaling. 
- Set the number of on-demand instances for critical tasks, then provision the rest as spot instances. 

# run EMR job

This is done by triggering Steps. 

3 ways to trigger EMR steps
- management console
- SSH into the primary node and manually run a task
- AWS CLI (recommended)

# EMR's compute platforms
 
- EC2: default. High performance and expensive
- EKS: light-weight app
- Serverless: small, low-code app.