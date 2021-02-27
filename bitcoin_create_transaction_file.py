import findspark
findspark.init()

import json
import pyspark
#from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, from_unixtime, to_date, sum,count, mean, countDistinct, round
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
import numpy as np
from bitcoin_utils import bitcoin_utils


#import bitcoin_utils

#from spark_config import spark, SparkContext
#class bitcoin_create_transaction_file:
    
#    def __init__(self, spark=None,sc=None):
#        if spark!=None:
#            self.spark=spark
#            self.sc=sc
        #else:
        #    self.spark = SparkSession \
        #    .builder \
        #    .getOrCreate()
        #    self.sc=self.spark.SparkContext
spark=None
SparkContext=None
            
class bitcoin_create_transaction_file:
    
    def __init__(self, spark_in=None):
        global spark
        global SparkContext
        self.utils=bitcoin_utils(spark_in)


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
            
            
    def _create_in_outs(self,x):
        to_return=[]
        input_ads=json.loads(x["inputs"])
        outputs=json.loads(x["outputs"])
        #sum_in=10
        #sum_out=9

        if len(input_ads)==0:
            fee=0
        else:
            sum_in = int(np.sum([int(x[3]) for x in input_ads]))
            sum_out = int(np.sum([int(x[2]) for x in outputs]))
            fee = sum_in-sum_out

        if len(input_ads)==0:
            input_ad="mining"
        else:
            input_ad=input_ads[0][2]#third element of the first address in input
            if len(input_ad)>1:#if multisig, keep as it is
                input_ad=str(input_ad)
            else:
                input_ad=input_ad[0] #take the unique address
        for output_ad_item in outputs:
            output_number=output_ad_item[0]
            output_ad=output_ad_item[1]
            output_val=output_ad_item[2]
            if len(output_ad)>1:
                output_ad=str(input_ad)
            else:
                output_ad=output_ad[0]
            to_return.append((x["hash"],output_number,input_ad,output_ad,output_val,x.timestamp,len(input_ads),len(outputs),fee))
        return(to_return)

    def createAd2Ad(self,output_dir,year_max="",month_max="",year_min="",month_min=""):
        #lines = spark.read.csv("/media/ssd2/bitcoinRemy/matched/data_part1_matched/transactions_00250000_00259999.json",sep="\t")
        #lines = spark.read.csv("/media/ssd2/bitcoinRemy/matched/*/*.json",sep="\t")
        lines = spark.read.load("/media/ed5/remyBitcoin/whole_dataset/")
        if year_max!="":
            if month_max=="":
                month_max=12
            lines = lines.filter(lines.year<=year_max).filter(lines.month<=month_max)
        if year_min!="":
            if month_min=="":
                month_min=1
            lines = lines.filter(lines.year>=year_min).filter(lines.month>=month_min)
        inputs = lines.select("hash","inputs","outputs","timestamp")
        test = inputs.rdd.flatMap(self._create_in_outs).toDF(["hash","output_id","src","dst","value","time","nb_inputs","nb_outputs","fee"])#.collect()#

        #.withColumn("date", to_date("date") \
        #test.withColumn("year", year("date")).withColumn("month", month("date")).withColumn("day", dayofmonth("date"))\
        #.write.partitionBy("year", "month","day") \
        test.write.mode("overwrite").save(output_dir)

        # Convert files from the minimal format to parquet files
        
        
    def create_initial_parquet_files(self):
        lines = spark.read.csv("/media/ssd2/bitcoinRemy/matched/*/*.json",sep="\t").toDF("hash","bloc","timestamp","fee","inputs","outputs")
        lines.withColumn("date", to_date(from_unixtime("timestamp"))) \
        .withColumn("year", year("date")).withColumn("month", month("date")).withColumn("day", dayofmonth("date"))\
        .write.partitionBy("year", "month","day").save("/media/ssd2/bitcoinRemy/whole_dataset")

    
        
        
    def append_identity(self,tr_ad2ad,ad2cl2identity,do_coalesce=False):
        #output_file1=output_file
        #if coalesce==True:
        #    output_file1=output_file+"temp"
        coalescing = spark.read.load(ad2cl2identity)
        coalescing= coalescing.withColumn("identity",coalesce(coalescing.identity,coalescing.cl)) 
        coalescing = coalescing.drop("cl")#.write.save(temp_dir+"/ad2actor")
        temp = self.utils.replace_column(tr_ad2ad,
                   coalescing,
                   None,
                   "src",
                   "ad",
                   ("identity","src_identity"),
                   replace=False,
                   write = False
                  )
        to_return = self.utils.replace_column(temp,
                   coalescing,
                   None,
                   "dst",
                   "ad",
                   ("identity","dst_identity"),
                   replace=False,              
                   write = False
                  )
        return to_return
        #if do_coalesce:
        #    coalescing = spark.read.load(output_file1)
        #    coalescing =coalescing.withColumn("src_identity",coalesce(coalescing.src_identity,coalescing.src)) 
        #    coalescing =coalescing.withColumn("dst_identity",coalesce(coalescing.dst_identity,coalescing.dst)) 
        #    coalescing.drop("src").drop("dst").drop("cl").write.save(output_file)



    def create_ad2identity(self,dir_temp,hints,ad2cl,output_file):
        replaced=self.utils.replace_column(hints,
               ad2cl,
               None,
               "adresse",
               "ad",
               replace=True,write=False
              )
        #spark.read.load(dir_temp+"/cls2identities")\
        replaced.na.drop().dropDuplicates(["service"]).sort("service").drop("adresse").withColumnRenamed("service","identity")\
        .write.save(dir_temp+"/cl2identity")

        self.utils.replace_column(ad2cl,
                   dir_temp+"/cl2identity",
                   output_file,
                   "cl",
                   "cl",
                   replace=False
                  )


        
        
    def append_USD_value(self,tr_file,btc_price_file,price_column):
        if isinstance(tr_file,str):
            tr_file=spark.read.load(tr_file)
        to_return = self.utils.replace_column(tr_file.withColumn("date",from_unixtime("time","yyyy-MM-dd")),
               btc_price_file,
               None,
               "date",
               "date",
               replace=False,
               write=False
              )
        to_return = to_return.withColumn("valueUSD", round(col("value")/100000000*col(price_column),3))
        return to_return


    #def create_tr_temporal_file(self,temp_dir,actor2actor_file,output_file):
    #    data_df = spark.read.load(temp_dir+"/cl2cl_price")\
    #    .withColumn("year", year("date")).withColumn("month", month("date")).withColumn("day", dayofmonth("date"))\
    #    .repartition("year", "month","day").drop("date").write.partitionBy("year", "month","day").save(output_file)
   # 
    
    
    
    def _inputs_details(self,x):
        to_return=[]
        input_ads=json.loads(x["inputs"])

        if len(input_ads)>0:
            for input_item in input_ads:
                input_hash=input_item[0]
                input_id=input_item[1]

                to_return.append((input_hash,input_id,x.timestamp,len(input_ads)))
        return(to_return)
                                 
    def create_spending_details(self,whole_dataset):
        inputs = whole_dataset.select("inputs","timestamp")
        to_return = inputs.rdd.flatMap(self._inputs_details).toDF(["input_hash","input_id","spent_timestamp","next_nb_inputs"])#.collect()#
        return to_return


    def append_spend_details(self,tr,spending_details):
        joined = tr.join(spending_details,
                         (tr["hash"] ==  spending_details["input_hash"]) & (tr["output_id"] == spending_details["input_id"]),how='left')
        return joined.drop("input_hash").drop("input_id")

    def append_categories(self,tr,actor2category):
        #rename actors to put the same name to all clusters of the same actor
        renamed = tr.withColumn("dst_2",regexp_extract(col('dst_identity'), '(.+?)((-(\d+$|(old.*$)))|$)', 1))\
        .withColumn("src_2",regexp_extract(col('src_identity'), '(.+?)((-(\d+$|(old.*$)))|$)', 1))
        #add categories

        temp=self.utils.replace_column(renamed,actor2category,"","dst_2","name",renaming_new_col=("cat","dst_cat"),write=False)
        with_categories=self.utils.replace_column(temp,actor2category,"","src_2","name",renaming_new_col=("cat","src_cat"),write=False)
        return with_categories
        
    def write_temporally(self,df,output_file):
        df.withColumn("year", year("date")).withColumn("month", month("date")).withColumn("day", dayofmonth("date"))\
        .repartition("year", "month","day").drop("date").write.partitionBy("year", "month","day").save(output_file)
        