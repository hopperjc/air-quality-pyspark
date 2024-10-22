from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

def load_data(file_path):
    spark = SparkSession.builder.appName("AirQualityPrediction").getOrCreate()
    data = spark.read.csv(file_path, header=True, inferSchema=True, sep=';')
    # Replace commas with dots to handle decimal values correctly
    for col_name in data.columns:
        data = data.withColumn(col_name, regexp_replace(col_name, ',', '.'))
    return data


if __name__ == "main":
    data = load_data("../data/air_quality.csv")
    data.show()