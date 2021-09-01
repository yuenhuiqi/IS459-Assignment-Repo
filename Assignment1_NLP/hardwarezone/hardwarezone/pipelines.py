# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
from scrapy.exceptions import DropItem
import re
import spacy
from spacy.lang.en import English
from spacy.lang.en.stop_words import STOP_WORDS

nlp = spacy.load('en_core_web_sm')
nlp.Defaults.stop_words -= {"my_stopword_1", "my_stopword_2"}
nlp.Defaults.stop_words |= {"my_stopword_1", "my_stopword_2"}


class HardwarezonePipeline:
    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            return item

class NewLineRemovalPipeline:
    def process_item(self, item, spider):
        newContent = []
        adapter = ItemAdapter(item)
        content = adapter["content"]
        for ch in content:
            ch = ch.replace("\t", " ")
            ch = ch.replace("\n", " ")
            ch = ch.strip()
            newContent.append(ch)
        adapter["content"] = newContent
        return item

class LightBoxRemovalPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        content = adapter["content"][0]
        adapter["content"] = re.sub("{.*?}", "", content)
        return item
        
class WhiteSpaceRemovalPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        content = adapter["content"]
        adapter["content"] = " ".join(content.split())
        return item

class RemoveStopwordsPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        content = nlp(adapter["content"])
        adapter["content"] = ' '.join([token.text for token in content if token.is_stop != True ])
        return item

class RemovePunctuationPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        content = nlp(adapter["content"])
        adapter["content"] = ' '.join([token.text for token in content 
            if token.is_punct != True and 
                token.is_quote != True and 
                token.is_bracket != True and 
                token.is_currency != True and 
                token.is_digit != True])
        return item

class LemmatizationPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        content = nlp(adapter["content"])
        adapter["content"] = ' '.join([word.lemma_ for word in content])
        return item

class MongoPipeline:
    collection_name = 'PC Gaming Forum Threads'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGODB_SERVER'),
            mongo_db=crawler.settings.get('MONGODB_DB')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
        return item