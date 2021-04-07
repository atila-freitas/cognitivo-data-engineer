from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

import pandas as pd
import json

def load_config():

    f = open('../config/types_mapping.json',)
    return json.load(f)

def apply_config_data_convert(spark_df, json_config):

    for each_data_name in json_config:
        if(json_config[each_data_name] == "integer"):
            spark_df = spark_df.withColumn(each_data_name, col(each_data_name).cast(IntegerType()))
        elif(json_config[each_data_name] == "timestamp"):
            spark_df = spark_df.withColumn(each_data_name, col(each_data_name).cast(TimestampType()))
        elif(json_config[each_data_name] == "date"):
            spark_df = spark_df.withColumn(each_data_name, col(each_data_name).cast(DateType()))
        elif(json_config[each_data_name] == "boolean"):
            spark_df = spark_df.withColumn(each_data_name, col(each_data_name).cast(BooleanType()))
        elif(json_config[each_data_name] == "string"):
            spark_df = spark_df.withColumn(each_data_name, col(each_data_name).cast(StringType()))
        elif(json_config[each_data_name] == "float"):
            spark_df = spark_df.withColumn(each_data_name, col(each_data_name).cast(FloatType()))

    return spark_df

def start_spark():

    conf = SparkConf().setMaster("local").setAppName("sparkproject")
    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)

    return sqlContext

def load_csv_to_spark(sqlContext):

    FullPath = "../data/input/users/load.csv"
    df = sqlContext.read.csv(FullPath, header=True)

    return df

def write_spark_to_csv(spark_df):

    FullPath = "../data/output/users"
    spark_df.write.mode('overwrite').options(header='True', delimiter=',').csv(FullPath)


def apply_deduplication(sqlContext, spark_df):

    spark_df.createOrReplaceTempView("users")
    query = """SELECT * FROM users us
        INNER JOIN (select id as last_row_id, MAX(update_date) as last_update from users GROUP BY id) 
        AS last_user_row 
        ON (last_user_row.last_row_id = us.id AND us.update_date = last_user_row.last_update)"""
    spark_df = sqlContext.sql(query).drop("last_row_id", "last_update")
    
    return spark_df


def main():
    sqlContext = start_spark()
    spark_df = load_csv_to_spark(sqlContext)
    json_config = load_config()

    spark_df = apply_deduplication(sqlContext, spark_df)
    spark_df = apply_config_data_convert(spark_df, json_config)
    spark_df.printSchema()

    write_spark_to_csv(spark_df)


if __name__ == "__main__":
    main()