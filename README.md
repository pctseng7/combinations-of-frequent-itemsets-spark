# MovieLens_Combinations_Frequent_Itemsets

Implementation of SON-Apriori Algorithm on MovieLens Ratings datasets to find combination of the frequent itemsets

## Getting Started

Dealing with big datasets via Spark can largely reduce processing time and get the insight in a short time. Follow the instructions will get you familiar with how to apply data-mining algorithm for large datasets. The open source datasets can be reached in the [MovieLens | GroupLens](https://grouplens.org/datasets/movielens/). In this repository, use the MovieLens 20M Dataset and MovieLens Latest Datasets for implementation. The goal is to find combination of the frequent itemsets. Small dataset can be used as testing dataset for the first try. 

## Data Mining

SON and [Apriori](https://en.wikipedia.org/wiki/Apriori_algorithm) algorithms are designed in this solution. Each task should also fulfill finding frequent moives/users, case1 and case2.

* Task1 - Implementing SON using Spark with the MovieLens Small Dataset

* Task2 - Implementing SON using Spark with the MovieLens Big Dataset

* Case1 - Find the combinations of frequent movies

* Case2 - Find the combinations of frequent users

## How to run my program

For testing use, the support for SON-Apriori Algorithm are provided below. 

### Task1 command

Case1 support 120

./bin/spark-submit --master local[*] Po-Chuan_Tseng.SON.py 1 ml-latest-small/ratings.csv 120

Case1 support 150

./bin/spark-submit --master local[*] Po-Chuan_Tseng.SON.py 1 ml-latest-small/ratings.csv 150

Case2 support 180

./bin/spark-submit --master local[*] Po-Chuan_Tseng.SON.py 2 ml-latest-small/ratings.csv 180

Case2 support 200

./bin/spark-submit --master local[*] Po-Chuan_Tseng.SON.py 2 ml-latest-small/ratings.csv 200

### Task2 command

Case1 support 29000

./bin/spark-submit --master local[*] Po-Chuan_Tseng.SON.py 1 ml-20m/ratings.csv 29000

Case1 support 30000

./bin/spark-submit --master local[*] Po-Chuan_Tseng.SON.py 1 ml-20m/ratings.csv 30000

Case2 support 2500

./bin/spark-submit --master local[*] Po-Chuan_Tseng.SON.py 2 ml-20m/ratings.csv 2500

Case2 support 3000

./bin/spark-submit --master local[*] Po-Chuan_Tseng.SON.py 2 ml-20m/ratings.csv 3000

## Credits

This repository is credited to the course project of INF553 at USC 
