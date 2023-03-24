 
# !/bin/bash
# Program:
# History:
# 03/11/2022

set cmd_prompt "]#|~]?"
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


cs4224o@xcnd39:/temp/cs4224o/Deployment$ cp -r results/ /temp/cs4224o/CQL
cs4224o@xcnd39:/temp/cs4224o/Deployment$ cp -r output/ /temp/cs4224o/CQL