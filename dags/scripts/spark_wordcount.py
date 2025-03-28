from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    sc = spark.sparkContext

    data = sc.parallelize([
        "hello world",
        "hello airflow",
        "spark is fast",
        "docker is cool"
    ])

    words = data.flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    wordCounts.saveAsTextFile("output_wordcount")

    spark.stop()