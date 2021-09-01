# Huiqi's IS459 Assignment 1

## About
In this repository, we'll be using Scrapy to crawl the threads & posts from the  HardwareZone PC Gaming Forum. The scraped data will then be stored into MongoDB.
<br> 
#### Directory 1: ```Assignment1``` (Default for submission)
This folder contains the codes necessary to crawl the data of Author, Title and Content from the HardwareZone PC Gaming Forum and store it into MongoDB.

#### Directory 2: ```Assignment1_NLP``` (For Surprise Me component)
This folder contains the codes to crawl the data of Author, Title and Content, and pass the content through additional pipelines to undergo Text Pre-processing through Normalization and Lemmatization of the Content of every post crawled. The new content will also be stored into MongoDB. This is made possible with the help of the spaCy library. 


## Setting up your virtual environment
Ensure that you already have the following installed before you begin:
> 1) MongoDB 5.0
> 2) Python 3 or later

The virtual environment has already been set up in this repository. To activate the virtual environment, run the following command:
```
source bin/activate
```
In the event you run into any issues with missing modules/libraries, run the following commands:
```
pip install Scrapy
pip install pymongo
pip install -U spacy
pip install -U spacy-lookups-data
python -m spacy download en_core_web_sm
```

## Scrapy & MongoDB Configurations
To ensure that the crawled data by Scrapy is successfully stored in MongoDB, do ensure that the connection configuration to MongoDB is correct. Navigate over to ```Assignment1/hardwarezone/hardwarezone/settings.py```
<br><br>By default, you should see the following configurations: 
```
MONGODB_SERVER = "localhost"
MONGODB_PORT = 27017
MONGODB_DB = "hardwarezone"
```
<br> Do change the port number if necessary. By default, it will be connected to MongoDB's connection port ```27017```.

## Initializing & Running Scrapy
Ensure that you are in the directory of ```Assignment1/hardwarezone/hardwarezone```. To start crawling the HardwareZone PC Gaming Forum, run the following command:
```
scrapy runspider spiders/spider.py
```
You will be able to view the crawled data being stored in MongoDB. 
