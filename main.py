import os
import platform

import pyspark.sql
from pyspark.pandas.spark.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, json_tuple, udf, explode
from pyspark.sql.types import MapType, IntegerType, StringType, ArrayType

import data_preprocessing, hash_url


def is_in_cluster():
    if platform.system() in ['Windows', 'Darwin']:
        return False
    else:
        hs = os.uname()[1]
        return True  # TODO fix later hs == node


def start_spark(app_name: str, in_cluster: bool = False):
    if in_cluster:
        # TODO fix up to needs
        spark = SparkSession.builder\
            .master("yarn") \
            .appName(app_name) \
            .config("spark.driver.memory", "90g") \
            .config("spark.executor.memory", "100g") \
            .config("spark.driver.cores", "32")\
            .config("spark.submit.deployMode", "client") \
            .config("spark.executor.heartbeatInterval", "50s") \
            .config("spark.network.timeout", "1000s") \
            .config("spark.executor.instances", "3") \
            .getOrCreate()

    else:
        spark = SparkSession.builder.master("local[8]") \
            .appName(app_name) \
            .config("spark.driver.memory", "9g").getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.codegen.wholeStage", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "6")

    spark.sparkContext.setLogLevel("ERROR")

    return spark


def preprocess_data(data: pyspark.sql.DataFrame, col_udf: str):
    split_udf = udf(lambda text: data_preprocessing.chunk_by_sentence_and_len(text),
                    MapType(IntegerType(), StringType()))
    data.columns.remove(col_udf)

    unchanged_cols = [col(c) for c in data.columns if c is not col_udf]  # FIXME

    data = data.select('url', 'timestamp', 'hash_url',
                       split_udf(col_udf).alias(col_udf))
    return data


def hash_url_md5(data: pyspark.sql.DataFrame, col_udf: str):
    hash_udf = udf(lambda text: hash_url.hash_md5(text))

    unchanged_cols = [col(c) for c in data.columns if c is not col_udf]  # FIXME

    data = data.select('url', 'timestamp', 'text',
                       hash_udf(col_udf).alias('hash_' + col_udf))
    return data


def run(path, cols):
    # Create spark session
    app_name = path.replace(".json", "")
    spark = start_spark(app_name=f"Pipeline_{app_name}", in_cluster=is_in_cluster())

    # Read data
    data = spark.read.json(path)

    # Hash url
    data = hash_url_md5(data, 'url')

    # Preprocess text
    data = preprocess_data(data, 'text')

    # Sentence id and populate, and add missing cols
    data = data.select('hash_url', 'url', 'timestamp', explode(data.text))\
        .withColumnRenamed('key', 'sentence_id')\
        .withColumnRenamed('value', 'sentence')\
        .withColumn("sentence_encoding", lit(None))

    data.show()
    print(data.schema)


if __name__ == '__main__':
    run('dummy_data/c4-train.00000-of-01024.json', ['url', 'text', 'timestamp'])
