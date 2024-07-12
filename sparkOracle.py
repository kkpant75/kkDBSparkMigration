#port forward
#ssh -L 1521:starburst-oracle-ur.czajb75xt9ny.us-west-2.rds.amazonaws.com:1521 ubuntu@bastionur

from pyspark.sql import SparkSession

# Path to the Oracle JDBC driver (replace with your actual path)
oracle_jar_path = "ojdbc8.jar"

# Create a Spark session
spark = SparkSession.builder \
    .appName("WriteToOracle") \
    .config("spark.driver.extraClassPath", oracle_jar_path) \
    .getOrCreate()

# Example DataFrame to write (replace with your actual DataFrame)
data = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
columns = ["id", "name"]
df = spark.createDataFrame(data, columns)
df.show()
# Oracle connection properties
#url = "jdbc:oracle:thin:@//starburst-oracle-ur.czajb75xt9ny.us-west-2.rds.amazonaws.com:1521/ORCL"
url = "jdbc:oracle:thin:@//localhost:1521/ORCL"
properties = {
    "user": "admin",
    "password": "Useready1",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Write DataFrame to Oracle
#df.write.jdbc(url=url, table="your_table_name", mode="overwrite", properties=properties)
df = spark.read.jdbc(url=url, table="EKS_QUERY_LOGGER.QUERY_TABLES", properties=properties)
df.show()
# Stop the Spark session
spark.stop()