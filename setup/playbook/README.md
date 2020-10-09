This Folder contains ansible playbooks for setting up the cluster.

Recommended specs for the cluster are:
1. assign ipv6 address for each instance, it's free and you don't need an elastic IP for each instances. But please do check if you have ipv6 connectivity.

After lauching your aws instances, you may take following steps to set up your cluster automatically:
Required steps:
1. ssh to your instances and accept servers' key
2. modify inventory.yml accordingly.
3. modify installApps.yml accordingly (IP address, hostname, packages).
4. modify changehostname.yml accordingly.
5. make sure you have following environment variables in your .profile or .bashrc:
```
export AWS_SECRET_ACCESS_KEY=xxxxxx
export AWS_ACCESS_KEY_ID=xxxxxx
export AWS_SSH_KEY=~/path/to/IAM-keypair.pem
export AWS_USER=USER
export DBUSER=USER
export DBPWD=PASSWORD
```
6. source your .profile or .bashrc

Recommended steps:
1. installApps.sh
   This script will provide following configuration:
   1. setup DNS64
   2. setup /etc/hosts
   3. setup AWS credential
   4. update system and install custome packages
   
   For spark sub group:
   1. setup spark/hadoop/aws/postgres jars
   
   For postgres sub group:
   1. pull postgis docker container
   2. install custome python packages

2. changehostname.sh: this script will modify hostnames of the cluster
3. config_storage.sh: if you setup extra storage attached to your servers, you may use this script to setup.
4. StartSparkCluster.sh: you may use this script to start your spark cluster.

If you receive error when installing rtree, you may consider install python3-rtree package provided by ubuntu instead.
