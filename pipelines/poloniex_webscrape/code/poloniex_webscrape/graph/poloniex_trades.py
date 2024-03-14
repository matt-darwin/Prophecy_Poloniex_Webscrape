from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from poloniex_webscrape.config.ConfigStore import *
from poloniex_webscrape.udfs.UDFs import *

def poloniex_trades(spark: SparkSession) -> DataFrame:
    from spark_ai.webapps import WebUtils
    WebUtils().register_udfs(spark)
    df1 = spark.range(1)

    return df1\
        .withColumn("url", lit(f"https://api.poloniex.com/markets/{Config.currency_pair}/trades?limit={Config.limit}"))\
        .withColumn("content", expr("web_scrape(url)"))
