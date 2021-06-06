# Build a SearchEngine With Pyspark
Here we try to build a search engine with pyspark without using use any existing libraries to compute TF-IDF or any builtin library to build the index or perform a search (e.g. elasticsearch)

**Part 1) Data Exploration**<br />
We have these Steps in our Final.py file:
- Upload the file: "shakespeare_full.json" to your linux machine and load its content to a dataframe df2.\
- Show the count of entries grouped by “speaker” on df2.\
**Output format:** Two columns: “speaker” and “count”

- Using spark.sql, show all entries where line_number starts with “1.1.” and text_entry contains the word “sometimes”.\
**Output format:** Use .show(), output columns: _id, speaker, line_number, text_entry

- Generate a list with the number of characters in every text entry where the speaker is “DONALBAIN”\
**Output format:** list of integers:  [X1, X2, …, Xn]

- Consider all text entries of the speaker “DONALBAIN”. Generate a list of pairs (key, value) where key is the _id of the text entry and value is the number of words in this text entry.\
**Output format:** [(key_1, value_1) , (key_2, value_2), … , (key_n, value_n)]

**Part 2)Building a search engine with pyspark**<br />
**[Building Index]** Compute TFIDF scores for all words in all text entries and build an inverted index. This index will be stored in the dataframe tokensWithTfIdf containing the following columns: (token, _id, tf, df, idf, tf_idf).\
token is any word in text entries, _id: text entry id, (tf,idf,tf_idf) scores of the pair (token,_id).\
Before creating the index, text entries must be converted to lower case and the punctuation signs removed.\
Cache the dataframe tokensWithTfIdf in memory for further usage.\
Show a sample 20 entries of your inverted index \
**Output format:** Use .show(). Output columns: token, _id, tf, df, idf, tf_idf**

**[Search]** Given a query and a value N, retrieve the top N matching text entries with their score (use TFIDF scores to retrieve the matching text entries)
You will construct a function search_words (query, N) where query is a string and N an integer. The result will display the top N text entries ordered by their score in descending order.\ The score is calculated using the formula <img width="348" alt="Screen Shot 2021-06-06 at 12 53 32 PM" src="https://user-images.githubusercontent.com/81987771/120933027-4e097900-c6c6-11eb-8a1d-796b75bbe246.png">

Show the results of each of the following queries, show three sets of results N=1, 3, 5:\
query1 = "to be or not"\
query2 = "so far so"\
query = "if you said so"\
**Output format:**  For each query, N=1,3,5 lines where every line is a tuple in the form: (_id, score, text_entry), score shown with 3 decimals. Example: (108782, 12.756, "As well as one so great and so forlorn")
