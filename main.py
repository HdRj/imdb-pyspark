
from pyspark import SparkConf
from pyspark.sql import SparkSession


def main():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("movies")
                     .config(conf=SparkConf())
                     .getOrCreate())

    path = "imdb-data/title.ratings.tsv.gz"

    movies_df = spark_session.read.csv(path)

    #movies_df.show()
    #movies_df.printSchema()


if __name__ == "__main__":
    main()
