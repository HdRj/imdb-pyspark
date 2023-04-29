import functions
import pyspark.sql.functions as f
from read_write import write
from pyspark.sql.window import Window

def task7(episode_df, basics_df):
    join_df = functions.join_basics_and_ratings(episode_df, basics_df)
    with_decades_df = add_decades(join_df)
    groups_df = groupby_decades(with_decades_df)
    result_df = groups_df.select('rank', 'originalTitle', 'startYear', 'averageRating')
    # result_df.show()
    # print(result_df.count())
    write(result_df, "results/task07")

def add_decades(df):
    result_df = df.filter(df.startYear.isNotNull()).withColumn('decade',  f.floor(f.col('startYear')/10))
    # result_df.show()
    # print(result_df.count())
    return result_df

def groupby_decades(df):
    window = Window.partitionBy('decade').orderBy(df['averageRating'].desc())
    result_df = df.select('*', f.row_number().over(window).alias('rank')).filter(f.col('rank') <= 10)
    # result_df.show(250)
    # print(result_df.count())
    return result_df