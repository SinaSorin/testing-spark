import sys
import re 

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')


"""
Task 2: Uk markerspaces dataset
In data folder there are two csv files:
  - uk-postcode: where you are especially interested in Postcode and Region columns to associate between the two pairs
  - uk-makerspaces-identificable-data: https://www.nesta.org.uk/archive-pages/uk-makerspaces-the-data/

 The purpose of your work is to say how many entries are in the dataset for each region recorded.
 E.g.
 Cardiff: 3
 Unknow: 2
 Swansea: 1
 Aberdeen 2

 You can make the connection between the postcode region and dataset by using the Postcode column in both csv files.
 Note1:  only the first part in the uk-makerspaces is actually the correct postcode in the other file.

 Note 2: Optimize the mapping using broadcast variables !: https://spark.apache.org/docs/latest/rdd-programming-guide.html#broadcast-variables
 


sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # Add the code here
    pass
