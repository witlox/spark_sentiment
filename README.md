# spark sentiment analysis of tweets

Twitter Sentiment Analysis using Apache Spark

Toying around with sentiment analysis using spark, this is all extremely experimental.

The idea is to analyze twitter data stored on hdfs, using the different techniques available to us.

You can specify a method which you want to run (or train):
- Machine learning:
    - Naive Bayes
    - Logistic Regression
    - RandomForrest
- WordScore
- CoreNLP (Stanford)

The machine learning models can be trained, initial binary classification models for Naive Bayes (.nb) and Logistic Regression (.lr) are already supplied in the models directory.
These models have been trained using the Sentiment140 dataset.

The WordScore method is a special case, it is a classifier using a word-score with positive and negative word lists.
These lists have been concatenated from various sources. The "train" method here actually extracts most common words from a training set.

The CoreNLP has 2 implementations which both use the Stanford libraries.
