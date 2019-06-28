import pymongo
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import AgglomerativeClustering
import math

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["bigdata"]
mycol = mydb["news"]

print('Total Record for the collection: ' + str(mycol.count()))

allData = pd.DataFrame(list(mycol.find()))

urlDict = pd.Series(allData.childUrl.values ,index=allData._id).to_dict()

vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(allData['news'])

parentUrls = len((allData.parentUrl).unique())
k = math.floor(allData.shape[0]/parentUrls) * 2
#clustering = AgglomerativeClustering(n_clusters=k, affinity="cosine",linkage="average").fit(X.toarray())
clustering = AgglomerativeClustering(n_clusters=k, affinity="euclidean",linkage="ward").fit(X.toarray())

clustered = clustering.labels_
print(clustered)
finalDict = {}
for i in range(len(clustered)):
    temp = urlDict.get(str(i))
    if clustered[i] in finalDict:
        finalDict[clustered[i]] = finalDict.get(clustered[i]) + " , " + temp
    else:
        finalDict[clustered[i]] = temp


for i in finalDict:
    print("Cluster no. = "+ str(i))
    temp = finalDict.get(i)
    temp_splitted = temp.split(" , ")
    for j in temp_splitted:
        print(j)
    print("----------------------------------------------")