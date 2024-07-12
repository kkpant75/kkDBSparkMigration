from pyspark.sql import SparkSession
#spark-submit --driver-class-path /path/to/mysql-connector-java-8.0.27.jar sparkmysql.py

mysql_jar_path ="C:\\Projects -Useready\\SparkLearning\\mysql-connector-java-8.0.27.jar"
# Create a Spark session
spark = SparkSession.builder \
    .appName("ReadMySQL") \
    .config("spark.driver.extraClassPath",mysql_jar_path ) \
    .getOrCreate()

#This also wokr as we passing driver from cmd
# spark = SparkSession.builder \
    # .appName("ReadMySQL") \
    # .getOrCreate()


# MySQL connection properties
url = "jdbc:mysql://localhost:3306/world"
properties = {
    "user": "root",
    "password": "mysql",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read the table into a DataFrame
df = spark.read.jdbc(url=url, table="city", properties=properties)

# Show the DataFrame
df.show()

# Print the schema
df.printSchema()
table_schema = df.schema
print(table_schema)

# Print the schema
print("Table Schema:")
table_schema.printTreeString()

# Stop the Spark session
spark.stop()