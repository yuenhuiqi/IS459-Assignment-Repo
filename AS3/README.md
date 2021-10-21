# Huiqi's IS459 Assignment 3

## About
In this repository, we'll be using Scrapy to crawl the threads & posts from the HardwareZone PC Gaming Forum. The scraped data will then be passed over to Kafka as a Kafka message, in the ```hwz-output``` topic. 
<br> 

## Setting up your environment
Ensure that you already have the following installed before you begin:
> 1) Python 3 or later
> 2) Kafka, Spark, Scrapy

## Starting up Kafka & Consuming Kafka Messages
Step 1: Open terminal and head over to your Kafka directory titled ```kafka_2.12-2.8.0```. If you are running on Mac, run the following command:
```
cd /usr/local/Cellar/kafka_2.12-2.8.0
```

Step 2: Start Zookeeper by running the following command: 
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Step 3: Open up another tab in terminal and repeat step 1. Afterwards, start up the Kafka environment by running the following command:

```
bin/kafka-server-start.sh config/server.properties
```

Step 4: Repeat Step 1 in another terminal tab. Start consuming messages that were output by the Scrapy crawling process by running the following command: 
```
bin/kafka-console-consumer.sh --topic hwz-output --from-beginning --bootstrap-server localhost:9092
```

Once this is done, view the next section on how to crawl data via Scrapy and output them into this Kafka topic. 

## Initializing & Running Scrapy
Ensure that you are in the directory of ```AS3/hardwarezone```. To start crawling the HardwareZone PC Gaming Forum, run the following command:
```
scrapy crawl hardwarezone
```
To view output of the crawling process, refer to the previous section Step 4 on how you can consume the Kafka messages. 
