from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from poloniex_webscrape.config.ConfigStore import *
from poloniex_webscrape.udfs.UDFs import *
from prophecy.utils import *
from poloniex_webscrape.graph import *

def pipeline(spark: SparkSession) -> None:
    df_poloniex_trades = poloniex_trades(spark)
    df_json_to_columns = json_to_columns(spark, df_poloniex_trades)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/poloniex_webscrape")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/poloniex_webscrape", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/poloniex_webscrape")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
