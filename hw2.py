from pyspark.sql.functions import col

df = spark.read.csv("clean.csv", header=True, inferSchema=True)

df_clean = df.filter(
    (col("topic").like("%Obesity%")) &
    (col("stratificationcategory1") == "Income")
)

df_clean.select("topic", "data_value", "stratification1").show(10, False)

# convert to numeric
df_clean = df_clean.withColumn(
     "data_value_num",
     col("data_value").cast("double")
).filter(
     col("data_value_num").isNotNull()
)

# remove "Data not reported"
df_clean = df_clean.filter(
    col("stratification1") != "Data not reported"
)

# FINAL result
df_result = df_clean.groupBy("stratification1") \
    .avg("data_value_num") \
    .orderBy("avg(data_value_num)", ascending=False)

df_result.show()

df_result.write.csv("spark_output", header=True)

