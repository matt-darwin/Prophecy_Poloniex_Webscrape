from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from poloniex_webscrape.config.ConfigStore import *
from poloniex_webscrape.udfs.UDFs import *

def json_to_columns(spark: SparkSession, in0: DataFrame) -> DataFrame:
    column_data = in0.withColumn("json_content", decode(col("content"), 'utf-8'))
    json_line_schema = StructType([
        StructField("id", IntegerType()),
        StructField("price", DecimalType(12.4)),
        StructField("quantity", DecimalType(12.4)),
        StructField("amount", DecimalType(12.4)),
        StructField("takerSide", StringType()),
        StructField("ts", DoubleType()),
        StructField("createTime", DoubleType())
    ])
    json_doc_schema = ArrayType(StringType())
    df = column_data\
             .withColumn("converted_json", from_json("json_content", json_doc_schema))\
             .withColumn("exploded_data", explode("converted_json"))\
             .withColumn("data", from_json("exploded_data", json_line_schema))\
             .withColumn("id_out", col("data.id"))\
             .withColumn("price_out", col("data.price"))\
             .withColumn("quantity_out", col("data.quantity"))\
             .withColumn("amount_out", col("data.amount"))\
             .withColumn("takerSide_out", col("data.takerSide"))\
             .withColumn("ts_out", col("data.ts"))\
             .withColumn("createTime_out", col("data.createTime"))
    out0 = df.select(
        df.id_out,
        df.price_out,
        df.quantity_out,
        df.amount_out,
        df.takerSide_out,
        df.ts_out,
        df.createTime_out
    )

    return out0
