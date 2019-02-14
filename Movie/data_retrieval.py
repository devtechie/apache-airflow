import pandas as pd

FILEPATH_MOVIE =r"~/Downloads/Project/Files/movie.csv"
FILEPATH_RATING = r"~/Downloads/Project/Files/rating.csv"
FILEPATH_TAGS = r"~/Downloads/Project/Files/tag.csv"


def get_data():
   df_movie = pd.read_csv(FILEPATH_MOVIE, nrows=10000)
   df_ratings = pd.read_csv(FILEPATH_RATING, nrows=10000)
   df_tags = pd.read_csv(FILEPATH_TAGS, nrows=10000)
   return df_movie, df_ratings, df_tags


def merge_data(df_movie, df_ratings, df_tags):
   df_movie_ratings =df_movie.merge(df_ratings, how="inner", on="movieId")
   df_movie_tags = df_movie.merge(df_tags, how="inner", on="movieId", )
   df_movie_tags_ratings = df_movie_ratings.merge(df_movie_tags, how="inner", on="movieId")
   return df_movie_ratings, df_movie_tags, df_movie_tags_ratings

