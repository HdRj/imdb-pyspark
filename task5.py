from read_write import write


def task5(akas_df,basics_df):
    akas_filtered_df = filter_akas(akas_df)
    basics_filtered_df = filter_basics(basics_df)
    joined_df=join_movies(akas_filtered_df,basics_filtered_df)
    result_df = group_and_sorted(joined_df)
    # result_df.show()
    # print(result_df.count())
    write(result_df, "results/task05")


def filter_basics(basics_df):
    result_df = basics_df.filter(basics_df.isAdult == 1).select(basics_df.tconst,basics_df.isAdult)
    # result_df.show()
    # print(result_df.count())
    return  result_df

def filter_akas(akas_df):
    result_df = akas_df.filter(akas_df.region != "\\N").select(akas_df.titleId,akas_df.region)
    # result_df.show()
    # print(result_df.count())
    return  result_df


def join_movies(akas_filtered_df,basics_filtered_df):
    result_df = (basics_filtered_df
                 .join(akas_filtered_df, basics_filtered_df.tconst == akas_filtered_df.titleId, how='inner'))
    # result_df.show()
    # print(result_df.count())
    return result_df

def group_and_sorted(df):
    result_df = df.groupby(df.region).count().orderBy('count', ascending=False).limit(100)
    # result_df.show()
    # print(result_df.count())
    return result_df