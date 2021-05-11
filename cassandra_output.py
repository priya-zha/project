from cassandra.cluster import Cluster
from pyspark.sql import SQLContext, SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

sc = SparkContext(appName="cassandra")
spark = SparkSession.builder.appName("Spark_cassandra").getOrCreate()
cluster = Cluster(contact_points=['172.31.64.191','172.31.68.237','172.31.64.174'])
session = cluster.connect('diabetesdb')
rows = session.execute("SELECT COUNT(*) FROM diabetesb")
count=rows[0]
print(count.count)
df = spark.createDataFrame(list(session.execute("SELECT * FROM diabetesb")))
df.show()
df.printSchema()


