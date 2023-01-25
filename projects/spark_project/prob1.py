import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import read_files

def spk(spark_session):
    # to read any file with header included
    foods, _, _, week1_sales, week2_sales = read_files.read_data_from_files()

    def quantity(foods, week):
        sorted_week_sales = week.groupBy("food_id").count()
        sorted_week_sales = sorted_week_sales.select(col("food_id"), col("count").alias("week_count"))
        joined_week_sales = foods.join(sorted_week_sales, "food_id", "inner").orderBy(col("week_count").desc())
        return joined_week_sales
    # WEEK 1
    joined_week1_sales = quantity(foods, week1_sales)
    joined_week1_sales = joined_week1_sales.select(col("food_name"), col("week_count").alias('Week1_Quantity'))
    # WEEK2
    joined_week2_sales = quantity(foods, week2_sales)
    joined_week2_sales = joined_week2_sales.select(col("food_name"), col("week_count").alias('Week2_Quantity'))
    # WEEK 1 & WEEK 2
    joined_week1_week2_sales = joined_week1_sales.join(joined_week2_sales, "food_name", "inner")
    joined_week1_week2_sales = joined_week1_week2_sales.select(col("food_name"), (col("week1_Quantity")+col("week2_Quantity")).alias('Total_Quantity'))
    joined_week1_week2_sales.show()
    # Writing data to files
    joined_week1_sales.limit(3).write.format('json').mode("append").option("header", "true").save("./prob1.json")
    joined_week2_sales.limit(3).write.format('json').mode("append").option("header", "true").save("./prob1.json")
    joined_week1_week2_sales.limit(3).write.format('json').mode("append").option("header", "true").save("./prob1.json")


if __name__ == '__main__':
    spark_session = SparkSession.builder.master("local").appName('Sample').getOrCreate()
    spk(spark_session)

