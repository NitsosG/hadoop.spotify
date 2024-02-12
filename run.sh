#!/bin/bash


# Execute Maven clean install
mvn clean package

docker exec namenode //bin//sh -c "hdfs dfs -mkdir -p /user/fnitsos/songs"
docker exec namenode //bin//sh -c "hdfs dfs -mkdir -p /user/hdfs/output/"
docker cp src/main/resources/songs30.csv namenode:/
docker cp src/main/resources/songs.csv namenode:/
docker exec namenode //bin//sh -c "hdfs dfs -put /songs30.csv /user/fnitsos/songs"
docker exec namenode //bin//sh -c "hdfs dfs -put /songs.csv /user/fnitsos/songs"
docker cp target/map.reduce-1.0-SNAPSHOT-jar-with-dependencies.jar namenode:/
docker exec namenode //bin//sh -c "hadoop jar /map.reduce-1.0-SNAPSHOT-jar-with-dependencies.jar"
docker exec namenode //bin//sh -c "rm -rf /output"
docker exec namenode //bin//sh -c "hdfs dfs -get /user/hdfs/output/ ."
docker cp namenode:/output .
cat output/*
