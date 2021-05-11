
#create a sparksession
from pyspark.sql import SparkSession
#import matplotlib.pyplot as plt
from sklearn.metrics import f1_score, recall_score, precision_score
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LinearSVC

spark = SparkSession.builder.appName("spark").getOrCreate()

#create spark dataframe
df = spark.read.csv("hdfs://master:9000/data/diabetic.csv", header=True,inferSchema=True)

#feature selection

assembler = VectorAssembler(inputCols=['Pregnancies','Glucose','BloodPressure','SkinThickness','Insulin','BMI','DiabetesPedigreeFunction','Age'],outputCol='features')
output_data = assembler.transform(df)

"""# TASK 5: Split Dataset & Build the Model"""

#create final data

final_data = output_data.select('features','Outcome')

#split the dataset ; build the model
train ,test = final_data.randomSplit([0.7,0.3])

"""# Logistic Regression"""

models = LogisticRegression(labelCol='Outcome')
model = models.fit(train)

#summary of the model
#summary = model.summary
#summary.predictions.describe().show()



evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction',labelCol='Outcome')

prediction_lr = model.transform(test)
predictions_lr_model = prediction_lr.toPandas()
print('Test Area Under PR: ', evaluator.evaluate(prediction_lr))
# Calculate and print f1, recall and precision scores
f1 = f1_score(predictions_lr_model.Outcome, predictions_lr_model.prediction)
recall = recall_score(predictions_lr_model.Outcome, predictions_lr_model.prediction)
precision = precision_score(predictions_lr_model.Outcome, predictions_lr_model.prediction)

print('Logistic Regression :F1-Score: {}, Recall: {}, Precision: {}'.format(f1, recall, precision))

type(prediction_lr)

"""# Decision Tree"""


dt = DecisionTreeClassifier(featuresCol = 'features', labelCol = 'Outcome', maxDepth = 4)
dtModel = dt.fit(train)
predictions = dtModel.transform(test)
predictions_dtc= predictions.toPandas()
print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))
# Calculate and print f1, recall and precision scores
f1 = f1_score(predictions_dtc.Outcome, predictions_dtc.prediction)
recall = recall_score(predictions_dtc.Outcome, predictions_dtc.prediction)
precision = precision_score(predictions_dtc.Outcome, predictions_dtc.prediction)

print('Decision tree :F1-Score: {}, Recall: {}, Precision: {}'.format(f1, recall, precision))

"""# Random Forest Classifier"""

rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'Outcome')
rfModel = rf.fit(train)
predictions_rfc = rfModel.transform(test)
#predictions.select('features', 'Outcome', 'rawPrediction', 'prediction', 'probability').show(10)
predictions_rfc_model= predictions_rfc.toPandas()
print("Test Area Under ROC: " + str(evaluator.evaluate(predictions_rfc, {evaluator.metricName: "areaUnderROC"})))
# Calculate and print f1, recall and precision scores
f1 = f1_score(predictions_rfc_model.Outcome, predictions_rfc_model.prediction)
recall = recall_score(predictions_rfc_model.Outcome, predictions_rfc_model.prediction)
precision = precision_score(predictions_rfc_model.Outcome, predictions_rfc_model.prediction)

print('Random forest classifier :F1-Score: {}, Recall: {}, Precision: {}'.format(f1, recall, precision))

"""# SVM"""



svm = LinearSVC(featuresCol = 'features', labelCol = 'Outcome')
model_svm = svm.fit(train)
predictions_svm = model_svm.transform(test)
predictions_svm_model= predictions_svm.toPandas()

print("Test Area Under ROC: " + str(evaluator.evaluate(predictions_svm, {evaluator.metricName: "areaUnderROC"})))
# Calculate and print f1, recall and precision scores
f1 = f1_score(predictions_svm_model.Outcome, predictions_svm_model.prediction)
recall = recall_score(predictions_svm_model.Outcome, predictions_svm_model.prediction)
precision = precision_score(predictions_svm_model.Outcome, predictions_svm_model.prediction)

print('SVM classifier :F1-Score: {}, Recall: {}, Precision: {}'.format(f1, recall, precision))