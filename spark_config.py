import findspark
findspark.init()

import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, from_unixtime, to_date, sum,count, mean, countDistinct
from pyspark import SparkContext
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
import numpy as np
#from bitcoin_utils import bitcoin_utils




spark= SparkSession \
    .builder \
    .config("spark.driver.memory", "100g") \
    .config("spark.executor.memory", "100g") \
    .config("spark.cores.max", "10") \
    .getOrCreate()
sc = spark.sparkContext
sc.addPyFile("bitcoin_create_transaction_file.py")
