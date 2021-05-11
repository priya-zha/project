from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from sklearn.metrics import classification_report, confusion_matrix

spark = SparkSession.builder.appName("spark").getOrCreate()

df = spark.read.parquet('hdfs://master:9000/data/diabetes_pq')

for i in df.columns[1:6]:
  data = df.agg({i:'mean'}).first()[0]
  print("Mean value for {} is {}".format(i,int(data)))
  df = df.withColumn(i,when(df[i]==0,int(data)).otherwise(df[i]))


assembler = VectorAssembler(inputCols=['Pregnancies','Glucose','BloodPressure','SkinThickness','Insulin','BMI','DiabetesPedigreeFunction','Age'],outputCol='feature')
output_data = assembler.transform(df)

#from pyspark.ml.feature import MinMaxScaler
scaler = MinMaxScaler(inputCol="feature", outputCol="features")
scalerModel = scaler.fit(output_data.select("feature"))
scaledData = scalerModel.transform(output_data)

"""# Build & Train Model"""

#from pyspark.ml.classification import LogisticRegression
final_data = scaledData.select('features','Outcome')

train , test = final_data.randomSplit([0.7,0.3])
models = LogisticRegression(labelCol='Outcome')
model = models.fit(train)

"""# Evaluation & Test Model"""

#from pyspark.ml.evaluation import BinaryClassificationEvaluator
predictions = model.evaluate(test)

evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='Outcome')
print(evaluator.evaluate(model.transform(test)))

print("Test Error = %g" % (1.0 - evaluator.evaluate(model.transform(test))))

#from sklearn.metrics import classification_report, confusion_matrix

y_true = predictions.predictions.select(['Outcome']).collect()
y_pred = predictions.predictions.select(['prediction']).collect()

print(classification_report(y_true, y_pred))
print(confusion_matrix(y_true,y_pred))

model.save("hdfs://master:9000/data/finalmodel")

#from pyspark.ml.classification import LogisticRegressionModel
#model = LogisticRegressionModel.load('model')



#create a new spark dataframe
#test_df = spark.read.csv("/content/files/newfile.csv",header=True,inferSchema=True)

#print the schema
#test_df.printSchema()

#create an additional feature merged column 
#test_data = ssembler.transform(test_df)

#print the schema
#test_data.printSchema()

#scalerM = scaler.fit(test_data.select("feature"))
#scaledD = scalerM.transform(test_data)

#scaledD.show()

#use model to make predictions
#results = model.transform(scaledD)
#results.printSchema()

#display the predictions
#results.select('features','prediction').show()
