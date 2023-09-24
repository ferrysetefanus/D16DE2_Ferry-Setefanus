from pyspark.sql import SparkSession
from pyspark import SparkContext

input_file = "gs://pub/shakespeare/rose.txt"
output_dir = "gs://us-central1-highcpu-11357220-bucket/output"

# Create a Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Create a Spark context
sc = SparkContext.getOrCreate()

# Read the input text file
lines = sc.textFile(input_file)

# Tokenize the lines into words
words = lines.flatMap(lambda line: line.split(" "))

# Map words to (word, 1) pairs for counting
word_count = words.map(lambda word: (word, 1))

# Reduce by key to count the occurrences of each word
word_count = word_count.reduceByKey(lambda a, b: a + b)

# Save the word count results to the specified output directory
word_count.saveAsTextFile(output_dir)

output_view = word_count.collect()
for (word, count) in output_view:
    print("%s: %i" % (word, count))

# Stop the Spark context
sc.stop()
