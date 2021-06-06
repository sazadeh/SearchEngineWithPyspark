
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *


#This function make build Inverse Document Frequency table 
def build_tf_idf_index(df2):
    #Here we remove all Punctuations and convert 'text_entry' to lower case.
    df2=df2.withColumn('text_entry',lower((regexp_replace('text_entry','[^\sa-zA-Z0-9]',''))))
    #Count the Number of documents which will be used in IDF calculation
    num_docs = df2.count()
    #Token is any word in "text_entry". This data frame is the basis for calculating TF and IDF
    words = df2.select('_id',explode(split(col("text_entry"), " ")).alias("token"))
    #Here we try to calculate TF explode "text_entry" to count the number of each word 
    pre_tf=words.groupBy("_id","token").count()
    # Here we do sum() aggregation of counts and set as column 'tf'
    tf=pre_tf.groupBy("_id","token", ).agg(sum(col('count')).alias("tf"))
    # Here we try to calculate DF , We calculate the number of documents word occued in them (DF)
    pre_idf = words.distinct().groupby("token").count()
    # Now calculate IDF based on this formula  log(num_docs / number of documents word occured in them (We define alias "idf" for that))
    idf = pre_idf.select(pre_idf.token,col("count").alias("df"), log10(num_docs/(col("count"))).alias("idf"))
    # Join TF with IDF dataframes (TF join IDF on word) so we have both TF and IDF for each words in "text_entry"
    pre_tf_idf = tf.join(idf, tf.token == idf.token, how='full') 
    # Calculate TF-IDF by multiplying TF * IDF (We define alias "tf_idf") and here we generate "tokensWithTfIdf" 
    tokensWithTfIdf  = pre_tf_idf.select(tf["token"],col("_id"),col("tf"),col("df"),col("idf"), (col("tf") * col("idf")).alias("tf_idf")) 
    #Cache the dataframe tokensWithTfIdf in memory for further usage.
    tokensWithTfIdf.cache()
    return(tokensWithTfIdf)

#This function helps to find the nearest match to "query"
def search_words(query,N,tokensWithTfIdf,df2):   
    # Count how many words are in the query 
    words_query = len(query.split(' '))
    #Convert query to a Dataframe
    words_df = sc.parallelize(query.split(" ")).map(lambda x:(x,)).toDF(["query_word"])
    # Here we define a "joined_df" data frame which is a joint between "tokenWithTfIdf" and "words_df" based on words similarity
    joined_df = tokensWithTfIdf.join(words_df, words_df.query_word == tokensWithTfIdf.token, "inner")
	#Here the number of matched words countes and aggregate(Sum) and find the score per word in query then join with df2 Data Frame to get "text_entry" from that
    pre_scored = joined_df.groupBy("_id").agg(count("*").alias("matched_words_qty"),sum(col('tf_idf')).alias("pre_score")).join(df2,joined_df._id==df2._id,"inner")
	# calculate the tf_idf score for the document as pre_score  based on this formula : (sum of per-word score) times number of matched words divided by number of words in query
    scored = pre_scored.select(df2._id, ((pre_scored.pre_score * pre_scored.matched_words_qty) / words_query).alias("matching_score"),pre_scored.text_entry)
    #Here we define "matching_score" to have 3 decimal number and sort the documents by descending matching score
    doc_matches = scored.withColumn("matching_score",format_number("matching_score",3)).sort("matching_score", ascending=False)
    # Here we return top N 
    doc_matches = doc_matches.limit(N)
    return doc_matches

# Here we send the query to "search_words" function and add the result in a List which is called 
def result_list(query,N,tokensWithTfIdf,df2,result):
    #Here we get match words from "search_words" function
    matches=search_words(query,N,tokensWithTfIdf,df2)
    #Here we make a list of match words based on N:1,2,3
    pre_result = matches.select("_id","matching_score","text_entry").rdd.map(lambda x:(x[0],x[1],x[2])).collect()
    #Here we add the pre_result to "result" list which contains all the top N text entries ordered by their score in descending order.
    result.append(pre_result) 
    return result 

def main(sc):
    sqlContext = SQLContext(sc)
    #At this part we load data from JSON file to "df2" Data Frame
    df2 = sqlContext.read.json("/user/root/lab/shakespeare_full.json") 
    #Search result for Query "to be or not" with N:1,2,3 will store in "match_list"
    match_list=[]
    #Search result for Query "so far so" with N:1,2,3 will store in "match_list1" 
    match_list1=[]
    #Search result for Query "so far so" with N:1,2,3 will store in "match_list2"
    match_list2=[]

    #Here we build Inverse Document Frequency table for df2 and put it in "tokensWithTfId"
    tokensWithTfIdf=build_tf_idf_index(df2)

    #Here we get the result from result_list function for Query "to be or not" with N:1,2,3 and try to put the results in "match_list"
    match_list = result_list("to be or not",1,tokensWithTfIdf,df2,match_list)
    match_list = result_list("to be or not",3,tokensWithTfIdf,df2,match_list)
    match_list = result_list("to be or not",5,tokensWithTfIdf,df2,match_list)

    #Here we get the result from result_list function for Query "so far so" with N:1,2,3 and try to put the results in "match_list1"
    match_list1 = result_list("so far so",1,tokensWithTfIdf,df2,match_list1)
    match_list1 = result_list("so far so",3,tokensWithTfIdf,df2,match_list1)
    match_list1= result_list("so far so",5,tokensWithTfIdf,df2,match_list1)
    
    #Here we get the result from result_list function for Query "if you said so" with N:1,2,3 and try to put the results in "match_list2"
    match_list2 = result_list("if you said so",1,tokensWithTfIdf,df2,match_list2)
    match_list2 = result_list("if you said so",3,tokensWithTfIdf,df2,match_list2)
    match_list2= result_list("if you said so",5,tokensWithTfIdf,df2,match_list2)
    
    #At this part we try to print the result for each Query which explained above
    print("************************** Search Result for  Query 'to be or not' *********************************************")
    print
    print(match_list)
    print
    print("*************************  Search Result for  Query 'so far so'  ***********************************************")
    print
    print(match_list1)
    print
    print("***************************  Search Result for 'if you said so'  ***********************************************")
    print
    print(match_list2)
    print
    print("****************************************************************************************************************")



if __name__  == "__main__":
    conf = SparkConf().setAppName("TF-IDF Indexer") # Set Spark configuration
    sc = SparkContext(conf = conf) # Set spark context with config
    main(sc)
    sc.stop()


#Command for running this file 
#spark-submit --master yarn-client --executor-memory 512m --num-executors 3 --executor-cores 1 --driver-memory 512m search.py
