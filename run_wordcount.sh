#!/bin/bash


# Execute Maven clean install
mvn clean package

docker exec namenode //bin//sh -c "hdfs dfs -mkdir -p /user/fnitsos/books"
docker exec namenode //bin//sh -c "hdfs dfs -mkdir -p /user/hdfs/output/"
docker cp src/main/resources/Shipwrecks.txt namenode:/
docker exec namenode //bin//sh -c "hdfs dfs -put /Shipwrecks.txt /user/fnitsos/books"
docker cp target/map.reduce-1.0-SNAPSHOT-jar-with-dependencies.jar namenode:/
docker exec namenode //bin//sh -c "hadoop jar /map.reduce-1.0-SNAPSHOT-jar-with-dependencies.jar"
docker exec namenode //bin//sh -c "rm -rf /output"
docker exec namenode //bin//sh -c "hdfs dfs -get /user/hdfs/output/ ."
docker cp namenode:/output .
cat output/*
