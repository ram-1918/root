import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def read_data_from_files():
    # to read any file with header included
    spark_session = SparkSession.builder.master("local").appName('Sample').getOrCreate()

    foods = spark_session.read.option("delimiter", ",").option("header", True).csv('foods.csv')
    customers = spark_session.read.option("delimiter", ',').option("header", True).csv('customers.csv')
    week1_sales = spark_session.read.option("delimiter", ',').option("header", True).csv('week_1_Sales.csv')
    week1_satisfaction = spark_session.read.option("delimiter", ',').option("header", True).csv('week_1_satisfaction.csv')
    week2_sales = spark_session.read.option("delimiter", ',').option("header", True).csv('week_2_Sales.csv')

    foods = foods.select(col("Food Id").alias("food_id"), col("Food Item").alias("food_name"), col("Price"))
    week1_sales = week1_sales.select(col("Customer ID").alias("customer_id"), col("Food ID").alias("food_id"))
    customers = customers.select(col("ID").alias("customer_id"), col("First Name").alias("firstname"), col("Last Name").alias("lastname"), col("Gender").alias("gender"), col("Company").alias("company"), col("Occupation").alias("occupation"),)
    week1_satisfaction = week1_satisfaction.select(col("Satisfaction Rating").alias("rating"))
    week2_sales = week2_sales.select(col("Customer ID").alias("customer_id"), col("Food ID").alias("food_id"))

    return foods, customers, week1_satisfaction, week1_sales, week2_sales
