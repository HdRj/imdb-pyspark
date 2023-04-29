from read_write import write


def task4(basics_df, name_df, principals_df):
    movies_df = get_movies_longer_120(basics_df)
    actors_df = filter_principals(principals_df)
    movies_actors_df = join_movies_actors(movies_df, actors_df)
    man_df = join_all(movies_actors_df, name_df)
    result_df = man_df.select(man_df.primaryName, man_df.primaryTitle, man_df.characters)
    # result_df.show()
    # print(result_df.count())
    write(result_df, "results/task04")


def get_movies_longer_120(basics_df):
    result_df = basics_df.filter(basics_df.runtimeMinutes > 120).select(basics_df.tconst, basics_df.primaryTitle)
    # result_df.show()
    # print(result_df.count())
    return result_df


def filter_principals(principals_df):
    result_df = (principals_df.filter(principals_df.category == 'actor')
                 .select(principals_df.tconst, principals_df.nconst, principals_df.characters))
    # result_df.show()
    # print(result_df.count())
    return result_df


def join_movies_actors(movies_df, actors_df):
    result_df = actors_df.join(movies_df, on='tconst', how='inner')
    # result_df.show()
    # print(result_df.count())
    return result_df


def join_all(movies_actors_df, name_df):
    result_df = movies_actors_df.join(name_df, on='nconst', how='left')
    # result_df.show()
    # print(result_df.count())
    return result_df
