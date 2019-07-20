from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
import pyspark

sc = pyspark.SparkContext()
# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(',')]
    return LabeledPoint(values[2], values[0:2])

data = sc.textFile("outt1.csv")
coin=data.map()
parsedData = data.map(parsePoint)

# Build the model
model = LinearRegressionWithSGD.train(parsedData, iterations=100, step=0.0001)
print(model.weights[0])
# Evaluate the model on training data
valuesAndPreds = parsedData.map(lambda p: (p.split(',')[0], model.predict(p.split(',')[1:3])))
# MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds.count()
# print("Mean Squared Error = " + str(MSE))

# Save and load model
model.save(sc, "myModelPath")
sameModel = LinearRegressionModel.load(sc, "myModelPath")
