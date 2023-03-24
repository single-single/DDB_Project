# DDB_Project
NUS Distributed Database Project
The zip file named Project_optimized is the latest version that we optimized the system, but not fully done, so it can not be entirely run smoothly, But it really boost up the speed of the transaction execution, So we attach it here. 

## Configuration Module
#### DB Config

```yaml
host:       "192.168.48.254"
port:       5433
dbName:     "yugabyte"
dbUser:     "yugabyte"
dbPassword: "yugabyte"
sslMode:    "disable"
sslRootCert: ""
```
User just need to change host and port to local config while deploying to servers, and due to the reason that we are going to read local xact file, we do not have to make 5 db config files any more.

#### Log config
```yaml
level:        "info"
outputPath:   "./test.log"
```
level can be selected from {error,info,debug}
and outputPath should be customized after deploying


#### General config
```yaml
nodeNum:        4
totalNode:      20
hasOutPutFile:  true
outPutPath:     "./output/stdout.txt"
isSQL:          1
```

in this struct, nodeNum means which node server the instance is on, and give the totalNumber of Xact files
hasOutPutFile and outPutPath control the standard output path of the system.
isSQL show whether the system is running on SQL or not.


## Log Module
The system will provide a global Log Handler to make it easier to generate log informations, The module implements 2 kinds of Writer, one for simple Message, and the other support format print.
Use with Log.INFO("log msg") to output a simple message
And Log.INFOf("log %s","file") to output a format message.

## Function Modules
Apart from those Modules to support the system, the project mainly have several parts: entity, transaction(sql), cql_transaction,performance_measurement,these modules give strong support to both interact with Database system and utilize the user whole are going to enlarge the system.


## Setup
#### Cluster node Setup

```shell
./bin/yb-master \
--master_addresses 192.168.48.254:7100,192.168.48.255:7100,192.168.51.0:7100 \
--rpc_bind_addresses 192.168.48.254:7100\
--fs_data_dirs "/temp/cs4224o" \
--replication_factor=3\
>& /temp/yb-master.out &

./bin/yb-tserver\
--tserver_master_addrs=192.168.48.254:7100,192.168.48.255:7100,192.168.51.0:7100\
--rpc_bind_addresses=192.168.48.254:9100\
--enable_ysql\
--pgsql_proxy_bind_address=192.168.48.254:5433\
--cql_proxy_bind_address=192.168.48.254:9042\
--fs_data_dirs=/temp/cs4224o/tserver/xcnd35\
>& /home/stuproj/cs4224o/work/yb-tserver.out &

```

## Getting Data
```shell
cd work/yugabyte-2.15.2.0/data/project_files/data_files
wget http://www.comp.nus.edu.sg/~cs4224/xact_files.zip
unzip xact_files.zip
```
## Creating Schema and loading data
First using scp command to upload schema.sql,import.sql,Schema_Final.cql to directory work/yugabyte-2.15.2.0/
For YSQL: 
```shell
./bin/ysqlsh -h 192.168.51.1 -f schema.sql
./bin/ysqlsh -h 192.168.51.1 -f import.sql
```
For YCQL:
```shell
./bin/ysqlsh 192.168.51.1 -f Schema_Final.cql
```
But the copy command may failed, a safe way is ./bin/ysqlsh 192.168.51.1
then copy the content in Schema_Final.cql section by section


## Benchmark
The command used is abstract like below
```shell
prefix="cs4224o@xcnd"
suffix=".comp.nus.edu.sg"

for (( i=35; i<=39; i++ ))
do
    hostname="$prefix$i$suffix"
    scp -r /Users/walskor/Code/Goland/Distributed_DB_Project $hostname:/temp/cs4224o/Deployment
    ssh $hostname > /dev/null 2>&1 <<aabbcc
    cd /temp/cs4224o/Deployment
    /home/stuproj/cs4224o/go/bin/go build Distributed_DB_Project
    ./Distributed_DB_Project
    exit
aabbcc
done
```
