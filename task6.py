from read_write import write


def task6(episode_df,basics_df):
    episode_gouped_df = group_and_sorted(episode_df)
    joined_df = join_name(episode_gouped_df, basics_df)
    result_df = joined_df.select(joined_df.originalTitle, 'count').orderBy('count', ascending=False)
    # result_df.show(50)
    # print(result_df.count())
    write(result_df, "results/task06")

def group_and_sorted(episode_df):
    result_df = episode_df.groupby(episode_df.parentTconst).count().orderBy('count', ascending=False).limit(50)
    # result_df.show(50)
    # print(result_df.count())
    return result_df

def join_name(episode_gouped_df, basics_df):
    result_df = (episode_gouped_df
                 .join(basics_df, episode_gouped_df.parentTconst == basics_df.tconst, how="inner"))
    # result_df.show(50)
    # print(result_df.count())
    return result_df