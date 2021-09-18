import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# for NLP of content:
from nltk import flatten
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, RegexpTokenizer

# import this to get spark to assign a unique ID to each author. 
from pyspark.sql.functions import monotonically_increasing_id
from graphframes import *

spark = SparkSession.builder.appName('sg.edu.smu.is459.assignment2').getOrCreate()

# Load data
posts_df = spark.read.load('hdfs://localhost:9000/user/huiqi/parquet-input/hardwarezone.parquet')

# Clean the dataframe by removing rows with any null value
posts_df = posts_df.na.drop()

# Find distinct users
author_df = posts_df.select('author').distinct()
print('Author number: ' + str(author_df.count())) #4661 distinct authors

# Assign ID to the users – DF of (author, id)
author_id = author_df.withColumn('id', monotonically_increasing_id())
author_id.show()

# creating first DF of (left topic, source author)
left_df = posts_df.select('topic', 'author').withColumnRenamed("topic","ltopic").withColumnRenamed("author","src_author")

# creating second DF of (right topic, destination author) – this DF is flipped from first DF.
right_df = left_df.withColumnRenamed('ltopic', 'rtopic').withColumnRenamed('src_author', 'dst_author')


# joining the 2 DF on same topic, to get connection between authors, 
# matching the different combination of authors. (with duplicates) --> can be (A1, A2) and (A2, A1) --> same connection, 2 edges. 
author_to_author = left_df.join(right_df, left_df.ltopic == right_df.rtopic).select(left_df.src_author, right_df.dst_author).distinct()
edge_num = author_to_author.count()
print('Number of edges with duplicate : ' + str(edge_num)) # 1499327

# replacing source author's name with IDs for author_to_author DF --> (dst author nane, source author id) --> (dst_author, src)
id_to_author = author_to_author.join(author_id, author_to_author.src_author == author_id.author).select(author_to_author.dst_author, author_id.id).withColumnRenamed('id','src')

# replacing destination author's name with IDs for id_to_author DF --> (source author id, dst author id) --> (src, dst)
id_to_id = id_to_author.join(author_id, id_to_author.dst_author == author_id.author).select(id_to_author.src, author_id.id).withColumnRenamed('id', 'dst')

# removing duplicates by filtering, only get matching records where src ID >= dst ID 
# eg: (A1, A2) and (A2, A1) --> this line will remove (A1, A2)
id_to_id = id_to_id.filter(id_to_id.src >= id_to_id.dst).distinct()

# remove edges where source author & destination author are same (aka author commenting on their on thread)
id_to_id.createOrReplaceTempView("idmatches")
sqlDF = spark.sql("select * from idmatches where src != dst")
sqlDF.cache()
print("Number of edges without duplicate :" + str(sqlDF.count()))  # 747333

# Build graph with RDDs --> graph is directed, depending on src & dst author. 
graph = GraphFrame(author_id, sqlDF)
# graph.edges.show() 
# graph.edges.count() # 751461

# For complex graph queries, e.g., connected components, you need to set
# the checkpoint directory on HDFS, so Spark can handle failures.
# Remember to change to a valid directory in your HDFS
spark.sparkContext.setCheckpointDir('hdfs://localhost:9000/user/huiqi/spark-checkpoint')




# Question 1: To find out how large are the communities. 

# Part 1 – Create DF of Connected Components of graph: (there's only component of 0 for all authors in the DF, meaning that it's highly connected?)
result = graph.connectedComponents()
result.orderBy("component", ascending=False).show() # showing in descending order
resultRDD = result.rdd
componentRDD = result.rdd.map(lambda x: (x[2], 1)).reduceByKey(lambda x, y: x+y) # map & reduce by key to get count of component 
componentDF = spark.createDataFrame(componentRDD).toDF("component", "count").sort(F.desc("count")) 

# Part 2 – retrieve author & content for every entry:
stopwordsTxt = spark.read.text('hdfs://localhost:9000/user/huiqi/stop_words_english.txt') #load stopwords
stopwordsList = stopwordsTxt.rdd.map(lambda row: row[0]).collect()
stopwordsList.extend(['1','2', '3', '4', '5', '6', '7', '8', '9', '10'])
tokenizer = RegexpTokenizer(r'\w+') # create tokenizer to remove special characters & stopwords
contentDF = posts_df.select('content').distinct() # initialize a DF with all content
combinedDF = contentDF.agg(F.collect_list("content").alias("content")) # combine all the lists of content
concatList = F.udf(lambda list: "".join(list), StringType()) # concat them into a string
concatContentDF = combinedDF.withColumn("content", concatList("content")) # adding it as a new column in a new DF
contentTokenRDD = concatContentDF.rdd.map(lambda x: (tokenizer.tokenize(x[0].lower()))) # convert to RDD & tokenize the content
contentListRDD = spark.sparkContext.parallelize(contentTokenRDD.first())  # get the list of words in content
contentRDD = contentListRDD.map(lambda x: (x, 1)).reduceByKey(lambda x, y: (x + y)) # finding count to every word
countDF = spark.createDataFrame(contentRDD).toDF("word", "count").sort(F.desc("count")) # sort in descending order
filteredCountDF = countDF.filter(countDF.word.isin(stopwordsList) == False) # remove if it's a stopword.
filteredCountDF.show()


# Question 2: How cohesive are the communities?

triangleCount = graph.triangleCount()
triCountDF = triangleCount.select("id", "count")
triCountDF.createOrReplaceTempView('triCount')
totalTrianglesDF = spark.sql("select sum(count) as total from triCount").select('total')
totalTriangles = totalTrianglesDF.collect()[0][0] # 264211899
avgTriangles = totalTriangles / author_df.count()


# Question 3: Is there any strange community? 

authorJoinDateDF = posts_df.select('author', 'joinDate').distinct() # rerieving year that each user joined
authorJoinDateDF.createOrReplaceTempView('join_date')
JoinDateDF = spark.sql("select author, substring(joinDate, -4, 4) as year from join_date order by year desc") # retrieves the DF of author, year sorted by most recent. 
JoinDateRDD = JoinDateDF.rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: (x + y)) # find count of every year
countYearDF = spark.createDataFrame(JoinDateRDD).toDF("year", "count").sort(F.desc("count"))
countYearDF.createOrReplaceTempView('countYear')
youngDF = spark.sql("select sum(count) from countYear where year >= 2011")
oldDF = spark.sql("select sum(count) from countYear where year < 2011")
youngDF.show() # total no. of authors under "younger generation"
oldDF.show() # total no. of authors under "older generation"



