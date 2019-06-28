# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from text_to_UD_JSON import texttoUDJson
import json
import pymongo

def consumeData(spark, topic):
    '''
    Consumer consumes tweets from producer
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer(topic)
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    # selects bigdata database
    mydb = myclient["bigdata"]
    # selects the news table
    mycol = mydb["news05072019"]
    idCount = 0
    for msg in consumer:
        temp = (msg.value).decode("utf-8").split("|*|")
        original = str(temp[2] + " " + temp[5] + " " + temp[6])
        parseTree = texttoUDJson(original)
        jsonData = json.dumps(parseTree)
        processedData = '[{ "_id" : ' + json.dumps(str(idCount)) + ',"parentUrl" : ' + json.dumps(
            str(temp[0])) + ',"childUrl" : ' + json.dumps(str(temp[1])) + ',"news" : ' + json.dumps(
            original) + ',"parseTree" : ' + jsonData + '}]'
        idCount = idCount + 1
        processedJson = json.loads(processedData)
        mycol.insert(processedJson)
        print(processedJson)
        print("")

if __name__ == "__main__":

    spark = SparkSession.builder \
        .master("local") \
        .appName("Crawler") \
        .getOrCreate()

    # Variables
    topic = "News"

    consumeData(spark, topic)
