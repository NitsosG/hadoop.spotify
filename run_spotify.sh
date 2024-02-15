#!/bin/bash


#Build the code into the jar
mvn clean package

#create the input folder in the hadoop file system
docker exec namenode //bin//sh -c "hdfs dfs -mkdir -p /user/fnitsos/songs"
#create the ouput folder in the hadoop file system
docker exec namenode //bin//sh -c "hdfs dfs -mkdir -p /user/hdfs/output/"
#copy songs file from the resources folder of the project to the root folder of the namenode container
docker cp src/main/resources/songs.csv namenode:/
#copy file from the root folder of the container in the hadoop file system
docker exec namenode //bin//sh -c "hdfs dfs -put /songs.csv /user/fnitsos/songs"
#Copy the executable(jar file) that was build before from the mvn clean package command into the namenode root folder
docker cp target/map.reduce-1.0-SNAPSHOT-jar-with-dependencies.jar namenode:/
#run the hadoop application
docker exec namenode //bin//sh -c "hadoop jar /map.reduce-1.0-SNAPSHOT-jar-with-dependencies.jar"
#remove container output folder because
docker exec namenode //bin//sh -c "rm -rf /output"
#Add the results of the hadoop application into the current docker container folder
docker exec namenode //bin//sh -c "hdfs dfs -get /user/hdfs/output/ ."
#Copy the applications output folder from the docker container into our project
docker cp namenode:/output .
#Print the results of the application
cat output/*
