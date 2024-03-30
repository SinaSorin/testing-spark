import sys
import re 

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')


"""
Task 1: Stackoverflow survey processing
Process the data from "data/2016-stack-overflow-survey-responses.csv" and output the following operations:
 - Count how many reponses are in total
 - Count how many reponses are from Canada
 - Count how many missing data for salary_midpoint are in the dataset 

 Use the broadcast variables and read the documentation to understand better why it is optimal to use them in counting
 https://spark.apache.org/docs/latest/rdd-programming-guide.html#broadcast-variables
"""

sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # Add the code here
    pass
