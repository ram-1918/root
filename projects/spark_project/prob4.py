import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import read_files

def spk(spark_session):
    # to read any file with header included
    foods, customers, _, week1_sales, week2_sales = read_files.read_data_from_files()

    def quantity(customers, foods, week):
        sorted_week_sales = week.groupBy("customer_id", "food_id").count()
        sorted_week_sales = sorted_week_sales.select(col("customer_id"), col("food_id"), col("count").alias("Quantity"))
        joined_week_sales = sorted_week_sales.join(customers, "customer_id", "inner")
        joined_week_sales = joined_week_sales.join(foods, "food_id", "inner")
        return joined_week_sales
    # WEEK 1
    joined_week1_sales = quantity(customers, foods, week1_sales)
    joined_week1_sales = joined_week1_sales.select(col('customer_id'), col("firstname"), col('lastname'), (col('Price')*col("Quantity")).alias('Total_Price1')).orderBy(col('Total_Price1').desc())
    joined_week1_sales.show()
    # WEEK2
    joined_week2_sales = quantity(customers, foods, week2_sales)
    joined_week2_sales = joined_week2_sales.select(col('customer_id'), col("firstname"), col("lastname"), (col('Price')*col("Quantity")).alias('Total_Price2')).orderBy(col('Total_Price2').desc())
    # WEEK 1 & WEEK 2
    joined_week1_week2_sales = joined_week1_sales.join(joined_week2_sales, ["customer_id", "firstname", "lastname"], "inner")
    joined_week1_week2_sales = joined_week1_week2_sales.select(col('customer_id'), col("firstname"), col('lastname'), (col("Total_price1")+col("Total_Price2")).alias('Total_Price')).orderBy(col('Total_Price').desc())
    joined_week1_week2_sales.show()
    # Writing data to files
    joined_week1_sales.limit(1).write.format('csv').mode("append").option("header", "true").save("./prob3.json")
    joined_week2_sales.limit(1).write.format('csv').mode("append").option("header", "true").save("./prob3.json")
    joined_week1_week2_sales.limit(1).write.format('csv').mode("append").option("header", "true").save("./prob3.json")


if __name__ == '__main__':
    spark_session = SparkSession.builder.master("local").appName('Sample').getOrCreate()
    spk(spark_session)

