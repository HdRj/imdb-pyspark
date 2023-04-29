def join_basics_and_ratings(basics_df, ratings_df):
    ratings_filtered_df = filter_ratings(ratings_df)
    result_df = (basics_df.join(ratings_filtered_df, on='tconst', how = 'inner')
                 .select('originalTitle', 'startYear', 'genres', 'averageRating'))
    # result_df.show(50)
    # print(result_df.count())
    return result_df

def filter_ratings(ratings_df):
    result_df = ratings_df.filter(ratings_df.averageRating.isNotNull())
    return result_df