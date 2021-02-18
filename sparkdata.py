import sys
import os
import pandas as pd
# os.environ[‘PYSPARK_SUBMIT_ARGS’] =  '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
# DOWNLOAD THE JAR FILES TO RUN IN AN OFFLINE MODE
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/priya/spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar pyspark-shell'
#Import dependencies
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.ml.classification import LogisticRegression
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import json
import pickle
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SQLContext, SparkSession

#Create Spark context
sc = SparkContext(appName="Sparkstreaming")
spark = SparkSession.builder.appName(
    "Spark-Kafka-Integration").master("local").getOrCreate()

#Create Streaming Context
ssc = StreamingContext(sc, 1)

#Connect to Kafka
kafka_stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "my-group", {"hospital": 1})
raw = kafka_stream.flatMap(lambda kafkaS: [kafkaS])
lines = raw.map(lambda xs: xs[1].split(","))

#Parse the inbound message as json
parsed = raw.map(lambda v: json.loads(v[1]))
authors_dstream = parsed.map(lambda data: (
    data['name'], data['age'], data['sex'], data['RBC_Count'], data['Platelets'], data['Neutrofils'], data['Basofils'], data['glucose'], data['bloodpressure'], data['skinthickness']))

def savetheresult(rdd):

    if not rdd.isEmpty():
        df = rdd.toDF(["name", "age", "sex", "RBC_Count", "Platelets","Neutrofils", "Basofils", "glucose", "bloodpressure", "skinthickness"])
        df.show()

        #since loading model didn't work so training model in each real time incoming data
        input_data = spark.read.csv('diabetes.csv', header=True, inferSchema=True)
        input_data = input_data.filter((input_data.Insulin != '0') & (input_data.Glucose != '0') & (input_data.BloodPressure != '0') & (input_data.BMI != '0'))
        assembler = VectorAssembler(inputCols=['Glucose', 'BloodPressure', 'SkinThickness', 'Age'], outputCol='features')
        output_data = assembler.transform(input_data)
            # output_data=output_data.dropna()
        final_data = output_data.select('features', 'Outcome')
        train, test = final_data.randomSplit([0.7, 0.3])
        model = LogisticRegression(labelCol='Outcome')
        model = model.fit(train)
        assembler = VectorAssembler(inputCols=['glucose', 'bloodpressure', 'skinthickness', 'age'], outputCol='features')
        test_data = assembler.transform(df)
        #model = pickle.load(open('models_limited.pickle', 'rb'))
        results = model.transform(test_data)
        results.select('name', 'prediction').show()

authors_dstream.foreachRDD(savetheresult)
parsed.pprint()
authors_dstream.pprint()

#Start the streaming context
ssc.start()
ssc.awaitTermination()
        #.write.save("final.json", format="json", mode="overwrite")
   
""".split())

      

        results.write.options(catalog=catalog).format("org.apache.spark.sql.execution.datasources.hbase").save()
authors_dstream.foreachRDD(savetheresult)
parsed.pprint()
# parsed = parsed.map(lambda data:Row(serial_id=getValue(str,data['name']), \
#		studentid=getValue(str,data['age']), \
#		url=getValue(str,data['glucose'])))

# a = parsed.map(lambda data: exec('global x; x = data['age']))
# print(df)




authors_dstream.pprint()
#data_df = pd.read_json('final.json/', lines=True)
# print('smt',data_df)
# data_df.pprint()

#Start the streaming context
ssc.start()
ssc.awaitTermination()
