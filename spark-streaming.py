# PySpark Program for Retail Data Analysis Project
# import of the required modules
# karthikmct@gmail.com
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# PySpark environment variable declarations
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python" 
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre/" 
os.environ["SPARK_HOME"]="/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/" 
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib" 
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip") 
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# Spark Session Context
spark = SparkSession \
    .builder \
    .appName("RetailDataAnalysisProject") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# Reading order data from Kafka sever provided Bootstrap
orderRaw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("startingOffsets", "latest") \
    .option("subscribe","real-time-project") \
    .load() \
    
# Defining schema for JSON format as per the expected incoming data
jsonSchema = StructType() \
    .add("invoice_no", LongType()) \
    .add("timestamp", TimestampType()) \
    .add("type", StringType()) \
    .add("country", StringType()) \
    .add("items", ArrayType(StructType([
    StructField("SKU", StringType()),
    StructField("title", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("quantity", IntegerType())]))) 
    
# Creating dataframe from input data after applying the schema
orderStream = orderRaw.select(from_json(col("value").cast("string"), jsonSchema).alias("order")).select("order.*")

# Defining functions to interpret the required columns
# is Order Checking
def is_order(order):
    return 1 if (order == "ORDER") else 0
# is return checking
def is_return(order):
    return 1 if (order == "RETURN") else 0
# Calculation of total items
def total_items(items):
    return len(items)
# Calculating the total cost of the times 
def total_cost(items, order_type):
    Sum = 0
    for item in items:
        Sum = Sum + item.unit_price * item.quantity if (order_type == "ORDER") else (
                Sum - (item.unit_price * item.quantity))
    return Sum

# UDF conversion 
isOrder = udf(is_order, IntegerType())
isReturn = udf(is_return, IntegerType())
totalItems = udf(total_items, IntegerType())
totalCost = udf(total_cost, DoubleType())

# Calculating columns - applying UDF
orderStream = orderStream.withColumn("is_order", isOrder(orderStream.type)) \
.withColumn("is_return", isReturn(orderStream.type)) \
.withColumn("total_items", totalItems(orderStream.items)) \
.withColumn("total_cost", totalCost(orderStream.items, orderStream.type))


# Write stream for console output as per the expectation (1 minute interval)
orderStream = orderStream.selectExpr("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order",
                                         "is_return")

orderOutputStream = orderStream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "False") \
    .trigger(processingTime="1 minute") \
    .start()

# Calculating time based KPIs
timeBasedKPIs = orderStream.withWatermark("timestamp", "1 minute") \
    .groupby(window("timestamp", "1 minute")) \
    .agg(count("invoice_no").alias("invoiceNo"),
        sum("total_cost").alias("totalcost"),
        sum("is_return").alias("return"),
        sum("is_order").alias("order")) \
     .withColumn("rate_of_return", col("return") / (col("return") + col("order"))) \
     .withColumn("avg_transaction", col("totalcost") / (col("return") + col("order")))

# write stream for time based KPIs
timeBasedKPIs = timeBasedKPIs.selectExpr("window", "invoiceNo", "totalcost", "avg_transaction",
                                             "rate_of_return")

timeBasedKPIsOutput = timeBasedKPIs \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/user/ec2-user/rdaproj/TimeBasedKPIsOutput") \
    .option("checkpointLocation", "/user/ec2-user/rdaproj/TimeBasedKPI") \
    .option("truncate", "False") \
    .trigger(processingTime="1 minute") \
    .start()


# Calculating time and country based KPIs
timeAndCountryBasedKPIs = orderStream.withWatermark("timestamp", "1 minute") \
    .groupby(window("timestamp", "1 minute"), "country") \
    .agg(count("invoice_no").alias("invoiceNo"),
        sum("total_cost").alias("totalCost"),
        sum("is_return").alias("return"),
        sum("is_order").alias("order")) \
    .withColumn("rate_of_return", col("return") / (col("return") + col("order")))

# write stream for time and country based KPIs
timeAndCountryBasedKPIs = timeAndCountryBasedKPIs.selectExpr("window", "country", "invoiceNo", "totalcost",
                                                                 "rate_of_return")


timeAndCountryBasedKPIsOutput = timeAndCountryBasedKPIs \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/user/ec2-user/rdaproj/TimeAndCountryBasedKPIsOutput") \
    .option("checkpointLocation", "/user/ec2-user/rdaproj/TimeAndCountryBased") \
    .option("truncate", "False") \
    .trigger(processingTime="1 minute") \
    .start()


# Waiting infinitely to read the data 
timeAndCountryBasedKPIsOutput.awaitTermination()
timeBasedKPIsOutput.awaitTermination()
orderOutputStream.awaitTermination()

