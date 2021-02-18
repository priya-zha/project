import sys
import os
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement
# os.environ[‘PYSPARK_SUBMIT_ARGS’] =  '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
# DOWNLOAD THE JAR FILES TO RUN IN AN OFFLINE MODE
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/priya/spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar pyspark-shell'
#Import dependencies
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.ml.classification import LogisticRegressionModel

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

        assembler = VectorAssembler(inputCols=['glucose', 'bloodpressure', 'skinthickness', 'age'], outputCol='features')
        test_data = assembler.transform(df)
# load the trained model      
        model = LogisticRegressionModel.load('finalmodel')
        results = model.transform(test_data)
        results.show()
        results.select('name', 'prediction').show()

        diabetes_list =[]

        for value in results.columns:

            finalvalue = results.select(value).first()[0]
            #print(mean_ratings)
            diabetes_list.append(finalvalue)



        #for i in range(0,len(diabetes_list)):
         #   print(diabetes_list[i])
     
	#cassandra part
        cluster = Cluster()
        session = cluster.connect('diabetesdb')
        #session.execute("INSERT INTO testing123 (id, city, name) VALUES (i, 'bob','hope')")

        batch=BatchStatement()

        for i in range(0,len(diabetes_list)):
           batch.add(SimpleStatement("INSERT INTO diabetesb(name, age, sex,RBC_Count,Platelets, Neutrofils,Basofils,glucose,bloodpressure,skinthickness,prediction) VALUES (%s, %s, %s ,%s, %s, %s ,%s, %s, %s ,%s,%s)"), (diabetes_list[0], diabetes_list[1],diabetes_list[2],diabetes_list[3],diabetes_list[4],diabetes_list[5],diabetes_list[6],diabetes_list[7],diabetes_list[8],diabetes_list[9],int(diabetes_list[13])))
        session.execute(batch)
        print("finished")
        #.write.save("final.json", format="json", mode="overwrite")
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
