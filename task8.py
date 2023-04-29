import functions
from read_write import write


def task8(episode_df,basics_df):
    join_df = functions.join_basics_and_ratings(episode_df, basics_df)
    #result_df =
    # result_df.show(50)
    # print(result_df.count())
    #write(result_df, "results/task08")