import datetime
import json
import re
from urllib.request import Request, urlopen
import numpy as np
import pandas as pd
import pymongo
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from newsplease import NewsPlease
from text_to_UD_JSON import texttoUDJson


def publish_message(producer_instance, topic_name, value):
    try:
        key_bytes = bytes('foo', encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), linger_ms=10)

    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def getLinks(url):
    USER_AGENT = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; de; rv:1.9.1.5) Gecko/20091102 Firefox/3.5.5'
    request = Request(url)
    request.add_header('User-Agent', USER_AGENT)
    response = urlopen(request)
    content = response.read()
    response.close()
    soup = BeautifulSoup(content, "html.parser")
    links = {}
    minLen = 50 # Setting the threshold for the minimum length of url
    social = ["facebook.com", "twitter.com", "instagram.com", "weather.com","plus.google.com", "linkedin.com", "youtube.com", "pinterest.com", "behance.net", "blog", ".pdf", ".mp3"] # Excluding urls containing these
    flag = 1
    for link in soup.findAll('a', attrs={'href': re.compile("^((https?://)|/)")}):
        if len(link.get('href')) > minLen:
            if "http" in link.get('href') or "www" in link.get('href'):
                finalURL = link.get('href')
            else:
                finalURL = url + link.get('href')
            for l in social:
                if l in finalURL:
                    flag = 0
            if links.get(finalURL) and flag == 1:
                links[finalURL] = links.get(finalURL) + 1
            elif flag == 1:
                links[finalURL] = 1
            flag = 1

    return links

def getData(url):
    article = NewsPlease.from_url(url)
    return article

if __name__ == "__main__":
    data = pd.read_csv('newssheet.csv')
    links = data.Links
    links = links.replace(np.nan, '', regex=True)
    if len(links) > 0:
        prod = connect_kafka_producer()

    # myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    # # selects bigdata database
    # mydb = myclient["bigdata"]
    # # selects the news table
    # mycol = mydb["news"]
    # idCount=0

    for link in links:
        if link:
            # print(link)
            try:  # need to open with try
                list = getLinks(link)
            except Exception as e:
                print(e)
                continue
            # except urllib.error.HTTPError as e:
            #     # if e.getcode() == 404:  # check the return code
            #     continue
            #     # raise  # if other than 404, raise the error
            # except urllib.error.URLError as u:
            #     print("---------------URL error------------------------")
            #     continue
            # except UnicodeEncodeError as ue:
            #     print("----------------Unicode error-------------------")
            #     continue

            for item in list:
                if (list[item] < 4): # checking the number of occurrence of the link in the website
                    # print(item)
                    try:  # need to open with try
                        article = getData(item)
                    except Exception as e:
                        print(e)
                        continue
                    # except urllib.error.HTTPError as e:
                    #     # if e.getcode() == 404:  # check the return code
                    #     continue
                    #     # raise  # if other than 404, raise the error
                    # except urllib.error.URLError as u:
                    #     print("---------------  URL child error----------------------------------------")
                    #     continue
                    # except UnicodeEncodeError as ue:
                    #     print("----------------Unicode error-------------------")
                    #     continue

                    if (article.date_publish == None):
                        continue
                    current_time = datetime.datetime.now()
                    current_date = current_time.strftime("%Y-%m-%d")
                    article_date = article.date_publish.strftime("%Y-%m-%d")

                    if (current_date != article_date):
                        continue

                    if (article.title == None or article.description == None or article.text == None):
                        continue

                    print("Url: "+ str(article.url))
                    print("Headline: "+ str(article.title))
                    print("authors: "+ str(article.authors))
                    print("Data Published: "+ str(article.date_publish))
                    print("Lead Paragraph: "+ str(article.description))
                    print("Text: "+ str(article.text))
                    print(" ")

                    publish_message(prod, 'News', (str(link)+'|*|'+str(article.url)+"|*|"+str(article.title)+'|*|'+str(article.authors)+"|*|"+str(article.date_publish)+"|*|"+str(article.description)+"|*|"+str(article.text)))

                    # original = str(article.title+" "+article.description+" "+article.text)
                    # temp = texttoUDJson(original)
                    # jsonData = json.dumps(temp)
                    # processedData = '[{ "_id" : ' + json.dumps(str(idCount)) + ',"parentUrl" : ' + json.dumps(str(link)) + ',"childUrl" : ' + json.dumps(str(article.url)) + ',"news" : ' + json.dumps(original) + ',"parseTree" : ' + jsonData + '}]'
                    # idCount= idCount + 1
                    # processedJson = json.loads(processedData)
                    # mycol.insert(processedJson)
                    # #print(processedJson)
                    # print("")
            print("")

    if prod is not None:
        prod.close()
