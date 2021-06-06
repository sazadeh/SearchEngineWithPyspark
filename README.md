# Build a SearchEngine With Pyspark
Here we try to build a search engine with pyspark without using use any existing libraries to compute TF-IDF or any builtin library to build the index or perform a search (e.g. elasticsearch)

**Part 1) Data Exploration**<br />
We have these Steps in our Final.py file:\
1.Upload the file: "shakespeare_full.json" to your linux machine and load its content to a dataframe df2.\
2.Show the count of entries grouped by “speaker” on df2.\
Output format: Two columns: “speaker” and “count”

3.Using spark.sql, show all entries where line_number starts with “1.1.” and text_entry contains the word “sometimes”.
Output format: Use .show(), output columns: _id, speaker, line_number, text_entry

4.Generate a list with the number of characters in every text entry where the speaker is “DONALBAIN”
Output format : list of integers:  [X1, X2, …, Xn]

5.Consider all text entries of the speaker “DONALBAIN”. Generate a list of pairs (key, value) where key is the _id of the text entry and value is the number of words in this text entry.
Output format: [(key_1, value_1) , (key_2, value_2), … , (key_n, value_n)]



**Part 2)Building a search engine with pyspark**<br />
