import pyspark.sql.functions as f

from read_write import write


def task2(df):
    result_df = df.filter((df.birthYear <= 1899) & (df.birthYear >= 1800)).select(df.primaryName)
    print(result_df.count())
    #result_df.show()
    write(result_df, "results/task02")