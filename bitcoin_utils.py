import findspark
findspark.init()

import json
import pyspark
#from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, from_unixtime, to_date, sum,count, mean, countDistinct
#from pyspark import SparkContext
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import IntegerType,StructField,StructType, LongType, FloatType
from pyspark.sql.functions import col, when

from pyspark.sql.functions import monotonically_increasing_id, coalesce, date_trunc, regexp_extract
import snap
from os import path
import glob

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


spark=None
SparkContext=None

class bitcoin_utils:
    
    def __init__(self, spark_in=None):
        global spark
        global SparkContext

        if spark_in!=None:
            spark = spark_in
        else:
            spark = SparkSession \
            .builder \
            .config("spark.driver.memory", "100g") \
            .config("spark.executor.memory", "100g") \
            .config("spark.cores.max", "10") \
            .getOrCreate()
        SparkContext= spark.sparkContext

    def replace_column(self,df_original,df_dict,output_file,key_original,key_dict,renaming_new_col=False,replace=False,write=True):
        """
        replace column key_original in df df_original by column target_name of df_dict, renamed as new_name
        """
        if isinstance(df_original,str):
            df_original = spark.read.load(df_original)
        if isinstance(df_dict,str):
            df_dict = spark.read.load(df_dict)

        df_original=df_original.withColumnRenamed(key_original,"key1")
        df_dict=df_dict.withColumnRenamed(key_dict,"key2")
        joined = df_original.join(df_dict,df_original["key1"] ==  df_dict["key2"],how='left')
        if replace:
            joined = joined.drop("key1")
        else:
            joined = joined.withColumnRenamed("key1",key_original)
        joined=joined.drop("key2")
        if renaming_new_col!=False:
            joined=joined.withColumnRenamed(renaming_new_col[0],renaming_new_col[1])
        if write:
            joined.write.save(output_file)
        return joined
    
    