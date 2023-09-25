"""
This script processes datasets to identify and display Marvel characters with the fewest connections
to other characters. The characters' connections are aggregated, and those with the minimum number
of connections are extracted. The resulting data is then joined with character names to provide
readable output. The top 50 characters with the fewest connections are displayed.

Dependencies:
- PySpark

Usage:
- Ensure that PySpark is installed and correctly configured.
- Modify the paths "file:///SparkCourse/Marvel-names.txt" and "file:///SparkCourse/Marvel-graph.txt"
  to point to the respective datasets.
- Run the script to view the characters with the fewest connections.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark_session = SparkSession.builder.appName("MarvelLoners").getOrCreate()

# Define schema for Marvel names
marvel_schema = StructType([
    StructField("character_id", IntegerType(), True),
    StructField("character_name", StringType(), True)
])

def read_marvel_data():
    """Return Marvel names and graph datasets as DataFrames."""
    names_df = spark_session.read.schema(marvel_schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names.txt")
    graph_lines_df = spark_session.read.text("file:///SparkCourse/Marvel-graph.txt")
    return names_df, graph_lines_df

def process_data(graph_lines_df):
    """Return a DataFrame of character IDs and their connection counts from the graph data."""
    connections_df = graph_lines_df.withColumn("character_id", func.split(func.trim(func.col("value")), " ")[0]) \
                                   .withColumn("connections_count", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
                                   .groupBy("character_id").agg(func.sum("connections_count").alias("connections_count"))
    return connections_df

def characters_with_min_connections(connections_df):
    """Return characters with the minimum number of connections and the connection count."""
    min_connection_count = connections_df.agg(func.min("connections_count")).first()[0]
    min_connections_df = connections_df.filter(func.col("connections_count") == min_connection_count)
    return min_connections_df, min_connection_count

def main():
    """Find and display Marvel characters with the fewest connections."""
    names_df, graph_lines_df = read_marvel_data()
    connections_df = process_data(graph_lines_df)
    min_connections_df, min_connection_count = characters_with_min_connections(connections_df)
    min_connections_with_names_df = min_connections_df.join(names_df, "character_id")
    print("The following characters have only " + str(min_connection_count) + " connection(s):")
    min_connections_with_names_df.select("character_name").show(50)
    spark_session.stop()

if __name__ == "__main__":
    main()
