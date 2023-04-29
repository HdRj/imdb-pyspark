from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t


import read_write
import task1
import task2
import task3

path01 = "imdb-data/name.basics.tsv.gz"
path02 = "imdb-data/title.akas.tsv.gz"
path03 = "imdb-data/title.basics.tsv.gz"
path04 = "imdb-data/title.crew.tsv.gz"
path05 = "imdb-data/title.episode.tsv.gz"
path06 = "imdb-data/title.principals.tsv.gz"
path07 = "imdb-data/title.ratings.tsv.gz"


def main():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("movies")
                     .config(conf=SparkConf())
                     .getOrCreate())

    #name.basics.tsv.gz
    #====================
    #nconst (string) - alphanumeric unique identifier of the name/person
    #primaryName (string)– name by which the person is most often credited
    #birthYear – in YYYY format
    #deathYear – in YYYY format if applicable, else '\N'
    #primaryProfession (array of strings)– the top-3 professions of the person
    #knownForTitles (array of tconsts) – titles the person is known for

    schema01=t.StructType([
        t.StructField("nconst", t.StringType(), True),
        t.StructField("primaryName", t.StringType(), True),
        t.StructField("birthYear", t.IntegerType(), True),
        t.StructField("deathYear", t.IntegerType(), True),
        t.StructField("primaryProfession", t.StringType(), True), #Array
        t.StructField("knownForTitles", t.StringType(), True) #Array
    ])

    #title.akas.tsv.gz
    #======================
    #titleId (string) - a tconst, an alphanumeric unique identifier of the title
    #ordering (integer) – a number to uniquely identify rows for a given titleId
    #title (string) – the localized title
    #region (string) - the region for this version of the title
    #language (string) - the language of the title
    #types (array) - Enumerated set of attributes for this alternative title. One or more of the following: "alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay". New values may be added in the future without warning
    #attributes (array) - Additional terms to describe this alternative title, not enumerated
    #isOriginalTitle (boolean) – 0: not original title; 1: original title

    schema02 = t.StructType([
        t.StructField("titleId", t.StringType(), True),
        t.StructField("ordering", t.IntegerType(), True),
        t.StructField("title", t.StringType(), True),
        t.StructField("region", t.StringType(), True),
        t.StructField("language", t.StringType(), True),
        t.StructField("types", t.StringType(), True), #Array
        t.StructField("attributes", t.StringType(), True),  #Array
        t.StructField("isOriginalTitle", t.BooleanType(), True)
    ])

    # title.basics.tsv.gz
    # =====================
    # tconst (string) - alphanumeric unique identifier of the title
    # titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
    # primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
    # originalTitle (string) - original title, in the original language
    # isAdult (boolean) - 0: non-adult title; 1: adult title
    # startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year
    # endYear (YYYY) – TV Series end year. ‘\N’ for all other title types
    # runtimeMinutes – primary runtime of the title, in minutes
    # genres (string array) – includes up to three genres associated with the title

    schema03 = t.StructType([
        t.StructField("tconst", t.StringType(), True),
        t.StructField("titleType", t.StringType(), True),
        t.StructField("primaryTitle", t.StringType(), True),
        t.StructField("originalTitle", t.StringType(), True),
        t.StructField("isAdult", t.BooleanType(), True),
        t.StructField("startYear", t.DateType(), True),
        t.StructField("endYear", t.DateType(), True),
        t.StructField("runtimeMinutes", t.IntegerType(), True),
        t.StructField("genres", t.StringType(), True) #Array
    ])

    #title.crew.tsv.gz
    #===================
    #tconst (string) - alphanumeric unique identifier of the title
    #directors (array of nconsts) - director(s) of the given title
    #writers (array of nconsts) – writer(s) of the given title

    schema04 = t.StructType([
        t.StructField("tconst", t.StringType(), True),
        t.StructField("directors", t.StringType(), True), #Array
        t.StructField("writers", t.StringType(), True)  # Array
    ])

    #title.episode.tsv.gz
    #tconst (string) - alphanumeric identifier of episode
    #parentTconst (string) - alphanumeric identifier of the parent TV Series
    #seasonNumber (integer) – season number the episode belongs to
    #episodeNumber (integer) – episode number of the tconst in the TV series

    schema05 = t.StructType([
        t.StructField("tconst", t.StringType(), True),
        t.StructField("parentTconst", t.StringType(), True),
        t.StructField("seasonNumber", t.IntegerType(), True),
        t.StructField("episodeNumber", t.IntegerType(), True)
    ])

    # title.principals.tsv.gz
    #==========================
    # tconst (string) - alphanumeric unique identifier of the title
    # ordering (integer) – a number to uniquely identify rows for a given titleId
    # nconst (string) - alphanumeric unique identifier of the name/person
    # category (string) - the category of job that person was in
    # job (string) - the specific job title if applicable, else '\N'
    # characters (string) - the name of the character played if applicable, else '\N'

    schema06 = t.StructType([
        t.StructField("tconst", t.StringType(), True),
        t.StructField("ordering", t.IntegerType(), True),
        t.StructField("nconst", t.StringType(), True),
        t.StructField("category", t.StringType(), True),
        t.StructField("job", t.StringType(), True),
        t.StructField("characters", t.StringType(), True)
    ])

    # title.ratings.tsv.gz – Contains the IMDb rating and votes information for titles
    # ===============================
    # tconst (string) - alphanumeric unique identifier of the title
    # averageRating – weighted average of all the individual user ratings
    # numVotes - number of votes the title has received

    schema07 = t.StructType([
        t.StructField("tconst", t.StringType(), True),
        t.StructField("averageRating", t.DoubleType(), True),
        t.StructField("numVotes", t.LongType(), True),
    ])

    name_df = read_write.read(path01, schema01, spark_session)
    akas_df = read_write.read(path02, schema02, spark_session)
    basics_df = read_write.read(path03, schema03, spark_session)
    crew_df = read_write.read(path04, schema04, spark_session)
    episode_df = read_write.read(path05, schema05, spark_session)
    principals_df = read_write.read(path06, schema06, spark_session)
    ratings_df = read_write.read(path07, schema07, spark_session)


    # name_df.show()
    # print(name_df.count())
    # name_df.printSchema()

    # akas_df.show()
    # print(akas_df.count())
    # akas_df.printSchema()
    #
    basics_df.show()
    print(basics_df.count())
    basics_df.printSchema()
    #
    # crew_df.show()
    # print(crew_df.count())
    # crew_df.printSchema()
    #
    # episode_df.show()
    # print(episode_df.count())
    # episode_df.printSchema()
    #
    # principals_df.show()
    # print(principals_df.count())
    # principals_df.printSchema()
    #
    # ratings_df.show()
    # print(ratings_df.count())
    # ratings_df.printSchema()

    #task1.task1(akas_df)
    #task2.task2(name_df)
    #task3.task3(basics_df)


if __name__ == "__main__":
    main()
