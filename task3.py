from read_write import write


def task3(df):
    result_df = df.filter(df.runtimeMinutes>120).select(df.primaryTitle)
    # print(result_df.count())
    # result_df.show()
    write(result_df, "results/task03")