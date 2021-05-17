from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

fakeFriends = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/fakefriends-header.csv")

# fakeFriends.createOrReplaceTempView("fakeFriends")

fakeFriends.groupBy("age").avg("friends").alias("avgFriends").sort("age").show()


spark.stop()
