from Movie.data_retrieval import get_data, merge_data
from Movie.connect import create_engine_pgsql, insert_to_db
from Movie.aggregation import agg_rating_movie, agg_rating_tag



df_movie, df_ratings, df_tags = get_data()
# print(df_movie[:5], df_ratings[:5],df_tags[:5])
df_movie_ratings, df_movie_tags, df_movie_tags_ratings = merge_data(df_movie, df_ratings, df_tags)
# print(df_movie_tags_ratings[:5])

df_agg_rating = agg_rating_movie(df_movie_ratings)
print(df_agg_rating[:10])
df_agg_rating_genre = agg_rating_tag(df_movie_tags_ratings)
print(df_agg_rating_genre[:50])

engine = create_engine_pgsql()
insert_to_db(engine, df_agg_rating, 'agg_movie_ratings')
insert_to_db(engine, df_agg_rating_genre, 'agg_rating_genre')



