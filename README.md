# StreamingKafkaDataWithSpark

This is a small sbt program that fetches some data from a kafka topic and with the help of spark streams , it streams the data to another kafka topic applying the various transformations in between.

### Prerequisites :

1) Zookeeper and kafka should be running
2) Create the topics 'inputTopic' and 'outputTopic'
3) Run the SimpleDStreamExample.scala class.
4) Run the SendDataToKakfa.scala class.
5) Consume from the 'outputTopic' to ensure that the data is being streamed.
