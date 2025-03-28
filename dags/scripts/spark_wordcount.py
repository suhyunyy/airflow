from pyspark import SparkContext

sc = SparkContext("spark://spark-master:7077", "SimpleTest")

data = sc.parallelize([1, 2, 3, 4, 5])
squared = data.map(lambda x: x * x).collect()

print("Squared numbers:", squared)