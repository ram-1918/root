import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import read_files


def spk(spark_session1):
    # Reading data from files
    foods, _, _, week1_sales, week2_sales = read_files.read_data_from_files()
    def total_amount(foods, week):
        joined_week_sales = foods.join(week, "food_id", "inner")
        aggregated_week_sales = joined_week_sales.groupBy("food_id", "food_name").agg(sum("Price").alias("total_amount"))
        return aggregated_week_sales

    # WEEK 1
    aggregated_week1_sales = total_amount(foods, week1_sales).select(col("food_name"), col("total_amount").alias("total_week1"))
    # WEEK 2
    aggregated_week2_sales = total_amount(foods, week2_sales).select(col("food_name"), col("total_amount").alias("total_week2"))
    # WEEK 1 & WEEK 2
    joined_week1_week2_sales = aggregated_week1_sales.join(aggregated_week2_sales, "food_name", "inner")
    joined_week1_week2_sales = joined_week1_week2_sales.select(col("food_name"), (col("total_week1")+col("total_week2")).alias('Total_Amount'))
    joined_week1_week2_sales.show()

    # Writing Data into file
    aggregated_week1_sales.limit(2).write.format('json').mode("append").option("header", "true").option('delimiter', '|').save("./prob2.csv")
    aggregated_week2_sales.limit(2).write.format('json').mode("append").option("header", "true").option('delimiter', '|').save("./prob2.csv")
    joined_week1_week2_sales.limit(2).write.format('json').mode("append").option("header", "true").option('delimiter', '|').save("./prob2.csv")


if __name__ == '__main__':
    spark_session1 = SparkSession.builder.master("local").appName('Sample2').getOrCreate()
    spk(spark_session1)
