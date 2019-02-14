import pandas as pd


def agg_rating_movie(df_movie_ratings):
    df = df_movie_ratings[['movieId', 'title', 'rating']]
    df_agg_rating = df.groupby(['movieId','title']).mean() \
        .sort_values('rating', ascending=False)
    return df_agg_rating[:50]


def agg_rating_tag(df_movie_tags):
    df = df_movie_tags[['tag', 'genres_x', 'rating']]
    df_agg_rating_tags = df.groupby(['tag', 'genres_x']).mean() \
        .sort_values(['rating'], ascending=False)
    return df_agg_rating_tags[:50]

