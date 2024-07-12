#spark-submit --driver-class-path mysql-connector-java-8.0.27.jar;ojdbc8.jar sparkReadWrite.py ------------windows
#spark-submit --driver-class-path mysql-connector-java-8.0.27.jar:ojdbc8.jar sparkReadWrite.py  -----------Unix
#port forward rds oralce
#ssh -L 1521:starburst-oracle-ur.czajb75xt9ny.us-west-2.rds.amazonaws.com:1521 ubuntu@bastionur


from pyspark.sql import SparkSession
# Create a Spark session

spark = SparkSession.builder \
    .appName("ReadWriteSQL") \
    .getOrCreate()


# MySQL connection properties
urlMySql = "jdbc:mysql://localhost:3306/world"
properties = {
    "user": "root",
    "password": "mysql",
    "driver": "com.mysql.cj.jdbc.Driver"
  
}

# Read the table into a DataFrame
df = spark.read.jdbc(url=urlMySql, table="city", properties=properties)

# Show the DataFrame
df.show()


# Oracle connection properties
#url = "jdbc:oracle:thin:@//starburst-oracle-ur.czajb75xt9ny.us-west-2.rds.amazonaws.com:1521/ORCL"
url = "jdbc:oracle:thin:@//localhost:1521/ORCL"
properties = {
    "user": "admin",
    "password": "Useready1",
    "driver": "oracle.jdbc.driver.OracleDriver",
    "characterEncoding": "utf8",
    "useUnicode": "true"
}

print("wriitng Oralce")
# Write DataFrame to Oracle
df.write.jdbc(url=url, table="EKS_QUERY_LOGGER.myTestRW", mode="overwrite", properties=properties)
#df = spark.read.jdbc(url=url, table="EKS_QUERY_LOGGER.QUERY_TABLES", properties=properties)
print("Rwading Oralce")
df = spark.read.jdbc(url=url, table="EKS_QUERY_LOGGER.myTestRW", properties=properties)
df.show()
# Stop the Spark session
spark.stop()