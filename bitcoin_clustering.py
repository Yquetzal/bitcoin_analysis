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
from bitcoin_utils import bitcoin_utils


import bitcoin_utils

class bitcoin_clustering:
    
    def __init__(self, spark=None):
        if spark!=None:
            self.spark=spark
        else:
            self.spark = SparkSession \
            .builder \
            .config("spark.driver.memory", "100g") \
            .config("spark.executor.memory", "100g") \
            .config("spark.cores.max", "10") \
            .getOrCreate()

    def _extract_in(x):
    to_return=[]
    inputs=json.loads(x.inputs)
    if len(inputs)>1:
        for i in range(len(inputs)-1):
            ads1=inputs[i][2]
            ads2=inputs[i+1][2]
            if len(ads1)==1:
                ads1=ads1[0]
            else:
                ads1=str(ads1)
            if len(ads2)==1:
                ads2=ads2[0]
            else:
                ads2=str(ads2)
            to_return.append((ads1,ads2))
        return(to_return)
    return([])
    def _create_co_spending(current_dir,year="",month=""):
        print("computing co_spending")
        lines = spark.read.load("/media/ssd2/bitcoinRemy/whole_dataset/")
        if year!="":
            lines = lines.filter(lines.year<=year).filter(lines.month<=month)
        inputs = lines.select("inputs")
        to_write = inputs.rdd.flatMap(_extract_in).toDF(["src","dst"])#.collect()#
        return(to_write)
    
    
    
    ###create a list of unique addresses to have int IDs
def _to_uniq_vals(row):
    return [(row.src,),(row.dst,)]


    
    
def _dfZipWithIndex (df, offset=1, colName="rowId"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe 
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''

    new_schema = StructType(
                    [StructField(colName,LongType(),True)]        # new added field in front
                    + df.schema.fields                            # previous schema
                )

    zipped_rdd = df.rdd.zipWithIndex()

    new_rdd = zipped_rdd.map(lambda args: ([args[1] + offset] + list(args[0])))

    return spark.createDataFrame(new_rdd, new_schema)

    def _create_ad2id(co_spending):
        addresses =co_spending.rdd.flatMap(_to_uniq_vals).toDF(["ad"]).distinct()
        ad2id = _dfZipWithIndex(addresses,colName="id")       
        return ad2id


    def _convert_co_spending_to_id(temp_dir,co_spending,ad2id,output_co_spending_file):
        replace_column(co_spending,
                   ad2id,
                   temp_dir+"/temp_co_spending",
                   "src",
                   "ad",
                   ("id","src_id"),
                   replace=True
                  )
        replace_column(temp_dir+"/temp_co_spending",
                   ad2id,
                   output_co_spending_file,
                   "dst",
                   "ad",
                   ("id","dst_id"),
                   replace=True
                  )
    

    def _write_clusters(clusters,output_file,ID=True,ID2A=None):
        output_file=open(output_file, "w+")
        for i,CnCom in enumerate(clusters):
            for nodeID in CnCom:
                if ID:
                    to_write=str(nodeID)
                else:
                    to_write=str(ID2A[nodeID])
                output_file.write(to_write+"\t"+str(i)+"\n")
        output_file.close()

    def _compute_with_snap(co_spend_id,id2cl_file):
        G0 = snap.LoadEdgeList(snap.TUNGraph, co_spend_id, 0,1)
        print("Number of Nodes: %d" % G0.GetNodes())
        print("Number of edges: %d" % G0.GetEdges())
        Components = G0.GetSccs()
        _write_clusters(Components,id2cl_file,True)


    def _convert_clusters_from_id_to_addresses(ad2id,id2cl,output):
        replace_column(ad2id,
                   id2cl,
                   output,
                   "id",
                   "id",
                   replace=True
                  )

    def compute_clusters(current_dir,year="",month=""):
        co_spending = _create_co_spending(current_dir,year,month)
        co_spending.write.save(current_dir+"/co_spending")

        co_spending=spark.read.load(current_dir+"/co_spending")
        ad2id = _create_ad2id(co_spending)
        ad2id.write.save(current_dir+"/ad2id")

        _convert_co_spending_to_id(current_dir,current_dir+"/co_spending",current_dir+"/ad2id",current_dir+"/co_spending_id")

        co_spend_id=spark.read.load(current_dir+"/co_spending_id")
        co_spend_id.coalesce(1).write.format("csv").option("header", "false").option("sep"," ").save(current_dir+"/co_spend_id.csv")

        co_spend_id_file = get_files_from_dir(current_dir+"/co_spend_id.csv","part")[0]

        _compute_with_snap(co_spend_id_file,current_dir+"/id2cl.ssv")
        spark.read.csv(current_dir+"/id2cl.ssv",sep="\t").toDF("id","cl").write.save(current_dir+"/id2cl")                    
        _convert_clusters_from_id_to_addresses(current_dir+"/ad2id",current_dir+"/id2cl",current_dir+"/ad2cl")