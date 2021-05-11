import sys
import os
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement
#import pandas as pd
# os.environ[‘PYSPARK_SUBMIT_ARGS’] =  '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
# DOWNLOAD THE JAR FILES TO RUN IN AN OFFLINE MODE
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/ubuntu/jar/spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar pyspark-shell'
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
sc = SparkContext(appName="batch")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.appName(
    "Spark-Kafka-Integration").getOrCreate()

#Create Streaming Context
ssc = StreamingContext(sc,30)

#Connect to Kafka
kafka_stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "my-group", {"diabetes": 1})
raw = kafka_stream.flatMap(lambda kafkaS: [kafkaS])
lines = raw.map(lambda xs: xs[1].split(","))

#Parse the inbound message as json
parsed = raw.map(lambda v: json.loads(v[1]))
authors_dstream = parsed.map(lambda data: (data['anonymous_name'], data['Pregnancies'], data['Glucose'], data['BloodPressure'], data['SkinThickness'], data['Insulin'], data['BMI'], data['DiabetesPedigreeFunction'],data['Age'],data['created_at'],data['location']))
j=0

#    data['name'], data['Age'], data['Glucose'], data['BloodPressure'], data['SkinThickness'], data['Insulin'], data['Pregnancies'], data['BMI'], data['DiabetesPedigreeFunction']))

def savetheresult(rdd):

    if not rdd.isEmpty():
       # df = rdd.toDF(["name", "Age", "Glucose", "BloodPressure", "SkinThickness", "Insulin", "Pregnancies", "BMI", "DiabetesPedigreeFunction"])
        df = rdd.toDF(["anonymous_name", "Pregnancies", "Glucose", "BloodPressure", "SkinThickness", "Insulin","BMI","DiabetesPedigreeFunction","Age","created_at","location"])

        
        df.show()

        #since loading model didn't work so training model in each real time incoming data
        input_data = spark.read.csv('hdfs://master:9000/data/diabetic.csv', header=True, inferSchema=True)
       # input_data = input_data.filter((input_data.Insulin != '0') & (input_data.Glucose != '0') & (input_data.BloodPressure != '0') & (input_data.BMI != '0'))
       # assembler = VectorAssembler(inputCols=['Glucose', 'BloodPressure', 'SkinThickness', 'Age'], outputCol='features')
        assembler = VectorAssembler(inputCols=['Pregnancies','Glucose','BloodPressure','SkinThickness','Insulin','BMI','DiabetesPedigreeFunction','Age'],outputCol='features')
        
        output_data = assembler.transform(input_data)
            # output_data=output_data.dropna()
        final_data = output_data.select('features', 'Outcome')
        train, test = final_data.randomSplit([0.7, 0.3])
        model = LogisticRegression(labelCol='Outcome')
        model = model.fit(train)

        #assembler = VectorAssembler(inputCols=['Glucose', 'BloodPressure', 'SkinThickness', 'Age'], outputCol='features')
        test_data = assembler.transform(df)
        #model = pickle.load(open('models_limited.pickle', 'rb'))
        results = model.transform(test_data)

        results.select('anonymous_name', 'prediction').show()
       # diabetes_list =[]
        #for value in results.columns:

         #   finalvalue = results.select(value).first()[0]
            #print(mean_ratings)
          #  diabetes_list.append(finalvalue)



        #for i in range(0,len(diabetes_list)):
         #   print(diabetes_list[i])


       # cluster = Cluster(contact_points=['172.31.64.191','172.31.68.237','172.31.64.174'])
       # session = cluster.connect('diabetesdb')
        #session.execute("INSERT INTO testing123 (id, city, name) VALUES (i, 'bob','hope')")

       # batch=BatchStatement()
        #studentlist=[(1,'ktm','ragini'), (2,'lalitpur','pr'),(3,'bhaktapur','aaru')]

       # for i in range(0,len(diabetes_list)):
        #    batch.add(SimpleStatement("INSERT INTO diabetesb(name, Age,Glucose,BloodPressure,SkinThickness,Insulin,BMI,Pregnancies,DiabetesPedigreeFunction,prediction) VALUES (%s, %s, %s ,%s, %s, %s ,%s, %s, %s ,%s,%s)"), (diabetes_list[0], diabetes_list[1],diabetes_list[2],diabetes_list[3],diabetes_list[4],diabetes_list[5],diabetes_list[6],diabetes_list[7],diabetes_list[8],diabetes_list[9],int(diabetes_list[13])))
       # session.execute(batch)
       # print("finished")

        #results.saveAsTextFile("/home/ubuntu/lol")
        

authors_dstream.foreachRDD(savetheresult)
#parsed.pprint()
#authors_dstream.pprint()

#Start the streaming context
ssc.start()
ssc.awaitTermination()
