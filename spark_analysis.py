import os
import configparser
from datetime import datetime
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.functions import col, udf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType,DateType

class SpotifyAnalysis:

    def __init__(self, conf_file='app.conf'):
        try:
            dir_path = os.path.dirname(os.path.realpath(__file__))
            options = self.get_config(os.path.join(dir_path, conf_file))
            if options:
                hdfs_read = options.get("hdfs_read")
                if hdfs_read:
                    self.file_url = options.get("hdfs_file_url")
                else:
                    self.file_url = options.get("local_file_location")
                self.db_host = options.get("db_host")
                self.db_user = options.get("db_user")
                self.db_password = options.get("db_password")
                self.db_database = options.get("db_database")
                sc = SparkContext.getOrCreate()
                self.spark = SparkSession(sc)
        except Exception as e:
            print(e)

    def get_config(self, conf_file):
        config = configparser.ConfigParser()
        config.read(conf_file)
        return config['options']

    def read_csv_file_create_schema(self):
        table_name = "spotify_data"
        pydfdf = None
        try:
            ss_schema = StructType([.add("sr.no",IntegerType(),True)
                                   .add("track_id ",StringType(),True)
                                   .add('track_name', StringType(), True)
                                   .add("track_popularity",IntegerType(),True)
                                   .add("duration_ms",IntegerType(),True)
                                   .add("explicit",IntegerType(),True)
                                   .add("release_date",IntegerType(),True)
                                   .add("danceability",FloatType(),True)
                                   .add("energy",FloatType(),True)
                                   .add("key",IntegerType(),True)
                                   .add("loudness",FloatType(),True)
                                   .add("mode",IntegerType(),True)
                                   .add("speechiness",FloatType(),True)
                                   .add("acousticness",FloatType(),True)
                                   .add("instrumentalness",FloatType(),True)
                                   .add("liveness",FloatType(),True)
                                   .add("valence",FloatType(),True)
                                   .add("tempo",FloatType(),True)
                                   .add("time_signature",IntegerType(),True)
                                   .add("artists_name",StringType(),True)
                                   .add("id_artists",StringType(),True)
                                   .add("followers",IntegerType(),True)
                                   .add("artists_popularity",IntegerType(),True)
                                   .add("genres",StringType(),True)])
            pydf = self.spark.read.csv(self.file_url, schema=ss_schema, header=True)

            if pydf:
                pydf.createOrReplaceTempView(table_name)
        except Exception as e:
            print(e)
        return pydfdf, table_name

    def store_data_in_database(self, df, table_name):
        try:
            df.write.format('jdbc').options(
                url='jdbc:mysql://{0}:3306/{1}'.format(self.db_host, self.db_database),
                driver='com.mysql.cj.jdbc.Driver',
                dbtable='{0}'.format(table_name),
                user='{0}'.format(self.db_user),
                password='{0}'.format(self.db_password)).mode('append').save()
            print("Data stored in table {0}".format(table_name))
        except Exception as e:
            print(e)

    def popular_genre(self, table_name):
        try:

            """1.--------------WHICH GENRE IS MOST POPULAR-------------"""
            _sql = """select artists_popularity,genres,artists_name,release_date from {0} order by artists_popularity desc""".format(
                table_name)
            df_popular_genre = self.spark.sql(_sql)
            df_popular_genre.show()
            if df_popular_genre:
                self.store_data_in_database(df_popular_genre, table_name="genre_table")
                except Exception as e:\
                    print(e)

    def popular_track(self, table_name):
        try:
            """2.--------------WHICH TRACK IS MOST POPULAR-------------"""
        _sql = """select track_popularity,genres,artists_name,release_date from {0} order by track_popularity desc""".format(
            table_name)
        df_popular_track = self.spark.sql(_sql)
        df_popular_track.show()
        if df_popular_track:
            self.store_data_in_database(df_popular_track, table_name="track_table")
            except Exception as e:
                print(e)


    def year_wise_popularity(self, table_name):
        try:
            """3.--------------YEAR WISE THE POPULAR OF THE SONG-------------"""
        _sql = """select artists_popularity,release_date,loudness,speechiness,liveness,valence,tempo from {0} order by artists_popularity desc""".format(
            table_name)
        df_year_wise_popularity = self.spark.sql(_sql)
        df_year_wise_popularity.show()
        if df_year_wise_popularity:
            self.store_data_in_database(df_year_wise_popularity, table_name="track_table")
            except Exception as e:
                print(e)


    def instruments_popularity_year(self, table_name):
        try:
            """4.--------------YEAR WISE THE POPULAR OF THE INSTRUMNETSALS SONG-------------"""
        _sql = """select release_date,instrumentalness from {0} order by release_date desc""".format(
            table_name)
        df_instruments_popularity_year = self.spark.sql(_sql)
        df_instruments_popularity_year.show()
        if df_instruments_popularity_year:
            self.store_data_in_database(df_instruments_popularity_year, table_name="track_table")
            except Exception as e:
                print(e)


    def year_genre_popular(self, table_name):
        try:
            """5.--------------YEAR ,GENRE IS POPULAR-------------"""
            _sql = """select release_date,genres,artists_popularity from {0} order by artists_popularity desc""".format(
            table_name)
            df_year_genre_popular = self.spark.sql(_sql)
            df_year_genre_popular.show()
            if df_year_genre_popular:
                self.store_data_in_database(df_year_genre_popular, table_name="track_table")
                except Exception as e:
                    print(e)



def analysis_all(self):
        try:
            pydf, table_name = self.read_csv_file_create_schema()
            if pydf and table_name:
                print("POPULAR GENRE...........")
                self.popular_genre(table_name)
                print("POPULAR TRACK...........")
                self.popular_track(table_name)
                print("year_wise_popularity...........")
                self.year_wise_popularity(table_name)
                print("instruments_popularity_year...........")
                self.instruments_popularity_year(table_name)
                print("year_genre_popular...........")
                self.year_genre_popular(table_name)





        except Exception as e:
            print(e)


if __name__ == "__main__":
    cls_obj = SpotifyAnalysis()
    cls_obj.analysis_all()
