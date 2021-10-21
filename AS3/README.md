# Huiqi's IS459 Assignment 3

## About
In this repository, we'll be using Scrapy to crawl the threads & posts from the HardwareZone PC Gaming Forum. The scraped data will then be passed over to Kafka as a Kafka message, in the ```hwz-output``` topic. Afterwards, we can then process the incoming message stream and gain meaningful insights by using Spark.
<br><br>

## Setting up your environment
Ensure that you already have the following installed before you begin:
> 1) Python 3 or later
> 2) Kafka, Spark, Scrapy
<br><br>

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

Once this is done, view the next section on how to crawl data via Scrapy and output them into this Kafka topic. <br><br>

## Initializing & Running Scrapy
Ensure that you are in the directory of ```AS3/hardwarezone```. To start crawling the HardwareZone PC Gaming Forum, run the following command:
```
scrapy crawl hardwarezone
```
To view output of the crawling process, refer to the previous section Step 4 on how you can consume the Kafka messages. <br><br>

## Running Python files for Spark Streaming

Disclaimer: For this assignment, it is assumed that there will be overlaps between each window (sliding window). As such, the top 10 words and authors gathered every 1 minute will consist of counts from overlapping windows. The results are also filtered such that the sliding window lies within the current timestamp. <br>
> <b> Example: If the current time is 12.03, it will show results from sliding window 12.02-12.04 and 12.03-12.05. It will NOT show results from 12.00-12.02 as the current time doesn't lie within the sliding window. </b>
<br>

### Configuring Spark Checkpoint
1) Change the checkpoint location in ```getTopAuthors.py``` and ```getTopWords.py``` to your own checkpoint location. 
2) Refresh your spark checkpoint by running the following commands:
```
hadoop fs -rm -r -f /user/<name>/spark-checkpoint
hadoop fs -mkdir /user/<name>/spark-checkpoint
```
### Output Results 

The results being output to the console consist of top 10 words and authors within a window that lies within the timestamp.


1) Get Top 10 Authors within 2 minute window, run the following command:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 getTopAuthors.py
```


2) Get Top 10 Words within 2 minute window, run the following command:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 getTopWords.py
```
  
3) To get BOTH Top 10 Authors and Top 10 Words, run the following command (If this doesn't output both dataframes, try step 1 and step 2 instead.): 
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 getTopAuthorsAndWords.py
```










