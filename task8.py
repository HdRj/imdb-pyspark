import functions
import pyspark.sql.functions as f
from read_write import write
from pyspark.sql.window import Window


def task8(episode_df, basics_df):
    join_df = functions.join_basics_and_ratings(episode_df, basics_df)
    split_df = split_rows(join_df)
    groups_df = groupby_genres(split_df)
    result_df = groups_df.select('rank', 'originalTitle', 'genres', 'averageRating')
    # result_df.show(300)
    # print(result_df.count())
    write(result_df, "results/task08")


def split_rows(df):
    result_df = df.withColumn('genres', f.explode(f.split('genres', ',')))
    # result_df.show(50)
    # print(result_df.count())
    return result_df

def groupby_genres(df):
    window = Window.partitionBy('genres').orderBy(df['averageRating'].desc())
    result_df = df.select('*', f.row_number().over(window).alias('rank')).filter(f.col('rank') <= 10)
    # result_df.show(250)
    # print(result_df.count())
    return result_df