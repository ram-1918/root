import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import read_files

def spk(spark_session):
    # to read any file with header included
    foods, customers, _, week1_sales, week2_sales = read_files.read_data_from_files()

    def quantity(customers, week):
        sorted_week_sales = week.groupBy("customer_id").count()
        sorted_week_sales = sorted_week_sales.select(col("customer_id"), col("count").alias("Quantity"))
        joined_week_sales = customers.join(sorted_week_sales, "customer_id", "inner").orderBy(col("Quantity").desc())
        return joined_week_sales
    # WEEK 1
    joined_week1_sales = quantity(customers, week1_sales)
    joined_week1_sales = joined_week1_sales.select(col('customer_id'), col("firstname"), col('lastname'), col("Quantity").alias('Quantity1'))
    joined_week1_sales.show()
    # WEEK2
    joined_week2_sales = quantity(customers, week2_sales)
    joined_week2_sales = joined_week2_sales.select(col('customer_id'), col("firstname"), col('lastname'), col("Quantity").alias('Quantity2'))
    # WEEK 1 & WEEK 2
    joined_week1_week2_sales = joined_week1_sales.join(joined_week2_sales, ["customer_id", "firstname", "lastname"], "inner")
    joined_week1_week2_sales = joined_week1_week2_sales.select(col("firstname"), col('lastname'), (col("Quantity1")+col("Quantity2")).alias('Total_Quantity')).orderBy(col('Total_Quantity').desc())
    joined_week1_week2_sales.show()
    # Writing data to files
    joined_week1_sales.limit(1).write.format('csv').mode("append").option("header", "true").save("./prob3.json")
    joined_week2_sales.limit(1).write.format('csv').mode("append").option("header", "true").save("./prob3.json")
    joined_week1_week2_sales.limit(1).write.format('csv').mode("append").option("header", "true").save("./prob3.json")


if __name__ == '__main__':
    spark_session = SparkSession.builder.master("local").appName('Sample').getOrCreate()
    spk(spark_session)

