import sys
from operator import add
import findspark
findspark.init()
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("first_data_preprocessing").getOrCreate()

data1 = [Row(col1='pyspark and spark', col2=1), Row(col1='pyspark', col2=2), Row(col1='spark vs hadoop', col2=2), Row(col1='spark', col2=2), Row(col1='hadoop', col2=2)]
df1 = spark.createDataFrame(data1)
df1.show()

R = Row("id","age","gender")
data2 = [[1, 2, 3],[20, 25, None],['male','female','male']]
df2 = spark.createDataFrame([R(*i) for i in zip(*data2)])
df2.show()
df2.printSchema()

#df3 = spark.read.csv('customer_data.csv', header=True, inferSchema=True)
#df3.count()
#len(df3.columns)
#df3.printSchema()


