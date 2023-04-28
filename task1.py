from read_write import write


def task1(df):
    df.filter().withColumn('language')

    write(df, "results/task1.csv")