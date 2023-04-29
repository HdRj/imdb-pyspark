from read_write import write


def task1(df):
    result_df = df.filter(df.language == "uk").select(df.title)
    # print(result_df.count())
    write(result_df, "results/task01")
