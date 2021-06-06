



from pyspark.sql import SQLContext
from pyspark.sql import Row
import json
import requests
from pyspark.sql.functions import *

#Part 1
#Upload the file: "shakespeare_full.json" to your linux machine and load its content to a dataframe df2
sqlContext = SQLContext(sc)
df2 = sqlContext.read.json("/user/root/lab/shakespeare_full.json") 
df2.show()

#Show the count of entries grouped by “speaker” on df2
df2.groupBy("speaker").count().show(100)

#Using spark.sql, show all entries where line_number starts with “1.1.” and text_entry contains the word “sometimes”. 
from pyspark.sql.functions import col, expr, when,lower,length,split,explode
df2.select('_id','speaker','line_number','text_entry').filter(col("line_number").startswith('1.1.') & (lower(col("text_entry")).contains('sometimes'))).show()
df2.select('_id','speaker','line_number','text_entry').filter(col("line_number").startswith('1.1.')).filter(lower(col("text_entry")).contains('sometimes')).show()



#Generate a list with the number of characters in every text entry where the speaker is “DONALBAIN”

df3=df2.select(length('text_entry').alias("List Of Integers")).filter(col("speaker")=='DONALBAIN').rdd.map(lambda x: x[0]).collect()
print("List of Integers = %s"%df3)

  
  
#Consider all text entries of the speaker “DONALBAIN”. Generate a list of pairs (key, value) where key is the _id of the text entry and value is the number of words in
#  this text entry. 

df3=df2.select('_id',size(split(col('text_entry'), ' ')).alias("Number of Words")).filter(col("speaker")=='DONALBAIN').rdd.collectAsMap()
df3.items()


#Part 2
sqlContext = SQLContext(sc)
    #At this part we load data from JSON file to "df2" Data Frame
df2 = sqlContext.read.json("/user/root/lab/shakespeare_full.json") 
   
    #Here we remove all Punctuations and convert 'text_entry' to lower case.
df2=df2.withColumn('text_entry',lower((regexp_replace('text_entry','[^\sa-zA-Z0-9]',''))))
    #Count the Number of documents which is used in IDF calculation
num_docs = df2.count()
    #Token is any word in "text_entry". This DF is the basis for calculating TF and IDF
words = df2.select('_id',explode(split(col("text_entry"), " ")).alias("token"))

    ########## In this part we will build Inverse Document Frequency table ##########

    #Here we try to calculate TF explode "text_entry" to count the number of each word 
pre_tf=words.groupBy("_id","token").count()
    # Here we do sum() aggregation of counts and set as column 'tf'
tf=pre_tf.groupBy("_id","token", ).agg(sum(col('count')).alias("tf"))
    # Here we try to calculate DF , We calculate the number of documents word occued in them (DF)
pre_idf = words.distinct().groupby("token").count()
    # Now calculate IDF based on this formula  log(num_docs / number of documents word occured in them (We define alias "DF" for that))
idf = pre_idf.select(pre_idf.token,col("count").alias("df"), log10(num_docs/(col("count"))).alias("idf"))
    # Join TF with IDF dataframes (TF join IDF on word) so we have both TF and IDF for per words in "text_entry"
pre_tf_idf = tf.join(idf, tf.token == idf.token, how='full') 
   
    # Calculate TF-IDF by multiplying TF * IDF
tokensWithTfIdf  = pre_tf_idf.select(tf["token"],col("_id"),col("tf"),col("df"),col("idf"), (col("tf") * col("idf")).alias("tf_idf")) 
    #Cache the dataframe tokensWithTfIdf in memory for further usage.
tokensWithTfIdf.cache()


def search_words(query,N,tokensWithTfIdf,df2):
#def search_words(query,N):    
    # Count how many words are in the query 
    words_query = len(query.split(' '))
    #Convert Query to a Dataframe
    words_df = sc.parallelize(query.split(" ")).map(lambda x:(x,)).toDF(["query_word"])
    # Here we define a "joined_df" data frame which is a joint between "tokenWithTfIdf" and "words_df" based on words similarity
    joined_df = tokensWithTfIdf.join(words_df, words_df.query_word == tokensWithTfIdf.token, "inner")
	#Here the number of matched words countes and aggregate(Sum) and find the score per word in query then join with df2 Data Frame to get "text_entry" from that
    pre_scored = joined_df.groupBy("_id").agg(count("*").alias("matched_words_qty"),sum(col('tf_idf')).alias("pre_score")).join(df2,joined_df._id==df2._id,"inner")
	# calculate the tf_idf score for the document as pre_score (summ of per-word score) times number of matched words divided by number of words in query
    scored = pre_scored.select(df2._id, ((pre_scored.pre_score * pre_scored.matched_words_qty) / words_query).alias("matching_score"),pre_scored.text_entry)
    #Here we define "matching_score" to have 3 decimal number and sort the documents by descending matching score
    doc_matches = scored.withColumn("matching_score",format_number("matching_score",3)).sort("matching_score", ascending=False)
    # Here we return top N 
    doc_matches = doc_matches.limit(N)
    # Here we show the result based on Triple ("_id","matching_score","text_entry")
    doc_matches = doc_matches.select("_id","matching_score","text_entry").rdd.map(lambda x:(x[0],x[1],x[2])).collect()
    return doc_matches

print("Search Result for 'to be or not' Query= %s" % match_list)
print
print("*********************************************")
print("Search Result for 'so far so' Query= %s" % match_list1)
print
print("*********************************************")
print
print("Search Result for 'if you said so' Query= %s" % match_list2)
print

    