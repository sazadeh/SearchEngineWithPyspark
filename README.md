# Build a Search Engine With Pyspark
Here we try to build a search engine with pyspark without using use any existing libraries to compute TF-IDF or any built-in library to build the index or perform a search (e.g. elasticsearch)

**Part 1) Data Exploration**<br />
We have these Steps in our "final.py" file:
- Upload the file: "shakespeare_full.json" to your linux machine and load its content to a dataframe df2.
- Show the count of entries grouped by “speaker” on df2.\
**Output format:** Two columns: “speaker” and “count”

- Using spark.sql, show all entries where line_number starts with “1.1.” and text_entry contains the word “sometimes”.\
**Output format:** Use .show(), output columns: _id, speaker, line_number, text_entry

- Generate a list with the number of characters in every text entry where the speaker is “DONALBAIN”\
**Output format:** list of integers:  [X1, X2, …, Xn]

- Consider all text entries of the speaker “DONALBAIN”. Generate a list of pairs (key, value) where **key** is the _id of the text entry and **value** is the number of words in this text entry.\
**Output format:** [(key_1, value_1) , (key_2, value_2), … , (key_n, value_n)]

**Part 2)Building a search engine with pyspark**<br />
Using dataframes in pyspark version 2, you will build a small search engine with TFIDF. The TFIDF scoring will apply only on the text_entry column.

**[Building Index]** Compute TFIDF scores for all words in all text entries and build an inverted index. This index will be stored in the dataframe **tokensWithTfIdf** containing the following columns: (token, _id, tf, df, idf, tf_idf).\
token is any word in text entries, **_id**: text entry id, **(tf,idf,tf_idf)** scores of the pair (token,_id).\
Before creating the index, text entries must be converted to lower case and the punctuation signs removed.\
Cache the dataframe **tokensWithTfIdf** in memory for further usage.\
Show a sample 20 entries of your inverted index \
**Output format:** Use .show(). Output columns: token, _id, tf, df, idf, tf_idf**

**[Search]** Given a query and a value N, retrieve the top N matching text entries with their score (use TFIDF scores to retrieve the matching text entries)
You will construct a function **search_words (query, N)** where query is a string and N an integer. The result will display the top N text entries ordered by their score in descending order.
The score is calculated using the formula:
<img width="348" alt="Screen Shot 2021-06-06 at 12 53 32 PM" src="https://user-images.githubusercontent.com/81987771/120933027-4e097900-c6c6-11eb-8a1d-796b75bbe246.png">

**Note:**\
Calculate the Score for a givven search query in all docuemnts based on above formula : (sum of tf_idf scores for each word of given query in each documents) times number of matched words between each docuemnts and Query divided by number of words in query.

Show the results of each of the following queries, show three sets of results N=1, 3, 5:\
query1 = "to be or not"\
query2 = "so far so"\
query = "if you said so"\
**Output format:**  For each query, N=1,3,5 lines where every line is a tuple in the form: (_id, score, text_entry), score shown with 3 decimals. Example: (108782, 12.756, "As well as one so great and so forlorn")

**Part 3)** Write a file **search.py** that you will run using spark-submit.\
#spark-submit --master yarn-client --executor-memory 512m --num-executors 3 --executor-cores 1 --driver-memory 512m search.py

**Output Result for the search part :**\
The result for given queries in our datset will be as follow:

<img width="1313" alt="Screen Shot 2021-06-09 at 2 26 14 PM" src="https://user-images.githubusercontent.com/81987771/121408818-ca9e9080-c92e-11eb-8cd1-2295ce2f610f.png">


**Recourses:**\
https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/\
https://stackoverflow.com/questions/40161879/pyspark-withcolumn-with-two-conditions-and-three-outcomes\
https://stackoverflow.com/questions/29988287/renaming-columns-for-pyspark-dataframes-aggregates\
https://stackoverflow.com/questions/44384102/why-agg-in-pyspark-is-only-able-to-summarize-one-column-at-a-time\
https://stackoverflow.com/questions/53218312/pyspark-how-to-remove-punctuation-marks-and-make-lowercase-letters-in-rdd\
http://www.learnbymarketing.com/1100/pyspark-joins-by-example/\
https://stackoverflow.com/questions/46707339/how-to-filter-column-on-values-in-list-in-pyspark?rq=1\

