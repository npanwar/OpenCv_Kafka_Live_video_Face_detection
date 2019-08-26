# OpenCv_Kafka_Live_video_Face_detection
Video face detection using AdaBoost model and streaming using kafka and spark
OpenCv_Kafka_Live_video_Face_detection


OpenCv_Kafka_Live_video_Face_detection
Camera Video Streaming through Kafka and Spark Stream and Face Detection using Opencv classifiers Realtime multi-camera video analytics system using Kafka, Spark, OpenCV, Java

Video face detection using AdaBoost model and streaming that video through kafka where spark stream collects and saves the face detected images

To run this repository on eclipse ide: Before running the code make sure to start zookeeper(local) and then the kafka broker server(local) First of all Run Zookeeper using command zkserver Start ZooKeeper by executing the command ./zkServer.sh start.

Then run kafka Go to the Kafka home directory and execute the command ./bin/kafka-server-start.sh config/server.properties.

create a topic for kafka consumer C:\kafka_2.10-0.10.2.0\bin\windows>kafka-topics.bat --create --zookeeper localho st:2181 --replication-factor 1 --partitions 1 --topic jbm Created topic "jbm".

Stop the Kafka broker through the command ./bin/kafka-server-stop.sh.
