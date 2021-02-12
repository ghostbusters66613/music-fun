from typing import AnyStr

from pyspark.sql.types import StructType, TimestampType, StringType
from pyspark.sql.functions import *
from pyspark.sql.window import Window


class DataReportGenerator:
    CSV_DEFAULT_DELIMITER = '\t'

    def __init__(self, spark):
        self._loader = DataLoader(spark)

    def top_tracks_for_top_sessions(self,
                                   input_file_path: AnyStr,
                                   output_file_path: AnyStr,
                                   top_tracks_number: int,
                                   top_sessions_number: int):

        data_df = self._loader.load(input_file_path, DataSchema())

        data_df = DataTransformer.add_previous_track_diff_mins(data_df)
        data_df = DataTransformer.add_previous_track_diff_mins(data_df)
        data_df = DataTransformer.add_new_session_indicator(data_df)
        data_df = DataTransformer.add_session_id(data_df)

        sessions = DataTransformer.extract_sessions_with_n_tracks(data_df)
        sessions = DataTransformer.rank_sessions_by_n_tracks(sessions)
        top_sessions = DataTransformer\
            .get_top_sessions(sessions, top_sessions_number)

        top_tracks_sessions = DataTransformer\
            .get_tracks_for_sessions(top_sessions, data_df)

        top_tracks = DataTransformer\
            .get_top_tracks(top_tracks_sessions, top_tracks_number)

        top_tracks = DataTransformer\
            .enrich_tracks_with_data(top_tracks, top_tracks_sessions)

        top_tracks = DataTransformer\
            .enrich_tracks_with_data(top_tracks, top_tracks_sessions)
        top_tracks = top_tracks.select('traid', 'artid', 'artname', 'traname')

        self._save_to_csv(top_tracks, output_file_path)

    def _save_to_csv(self, data_df, file_path,
                   delimiter: AnyStr = CSV_DEFAULT_DELIMITER,
                     header: bool = True):
        data_df.coalesce(1).write.format('csv') \
            .mode('overwrite') \
            .options(delimiter=delimiter) \
            .options(header=header) \
            .save(file_path)


class DataTransformer:
    """Responsible for different data transformation
    """
    SECONDS_IN_MIN = 60

    @staticmethod
    def add_previous_track_diff_mins(data_df):
        """Add minutes since the previous track was played

        Args:
            data_df:

        Returns:
            data_df
        """
        window_spec = Window. \
            partitionBy("userid"). \
            orderBy("timestamp")

        diff_secs_col = col("timestamp").cast("long") - \
                        lag("timestamp", 1).over(window_spec).cast("long")

        return data_df.withColumn(
                "previous_track_tms_mins",
                diff_secs_col / DataTransformer.SECONDS_IN_MIN
        )

    @staticmethod
    def add_new_session_indicator(data_df):
        """Add an indicator to mark a new
        session(time between tracks > 20 mins) for a user

        Args:
            data_df:

        Returns:
            data_df
        """
        return data_df \
            .withColumn(
                'is_new_session',
                when(col('previous_track_tms_mins') > 20, 1).otherwise(0)
        )

    @staticmethod
    def add_session_id(data_df):
        """Add session id

        Args:
            data_df:

        Returns:
            data_df
        """
        window_spec = Window.partitionBy('userid').orderBy('timestamp')

        return data_df \
            .withColumn(
                "user_session_id",
                sum('is_new_session').over(window_spec)
        )

    @staticmethod
    def extract_sessions_with_n_tracks(data_df):
        """Group by sessions and calculate tracks count

        Args:
            data_df:

        Returns:

        """
        return data_df \
            .groupBy('userid', 'user_session_id') \
            .agg(count("*").alias('tracks_cnt'))

    @staticmethod
    def rank_sessions_by_n_tracks(sessions_df):
        """Rank sessions by number for tracks

        Args:
            sessions_df:

        Returns:

        """
        window_spec = Window.orderBy(col('tracks_cnt').desc())

        return sessions_df \
            .withColumn(
                'sessions_rank',
                row_number().over(window_spec)
        )

    @staticmethod
    def get_top_sessions(sessions_df, number_of_sessions):
        """Get the top N sessions

        Args:
            sessions_df:
            number_of_sessions:

        Returns:

        """
        return sessions_df \
            .filter(sessions_df.sessions_rank <= number_of_sessions)

    @staticmethod
    def get_tracks_for_sessions(sessions_df, tracks_df):
        """Get tracks for sessions

        Args:
            sessions_df:
            tracks_df:

        Returns:

        """
        return tracks_df \
            .join(sessions_df, ["userid", 'user_session_id'], "leftsemi")

    @staticmethod
    def get_top_tracks(data_df, tracks_number):
        """Get top N tracks

        Args:
            data_df:
            tracks_number:

        Returns:

        """
        top_tracks = data_df \
            .groupBy("traid") \
            .agg(count('traid').alias('track_cnt_in_sessions'))

        return top_tracks \
            .orderBy(col("track_cnt_in_sessions").desc()) \
            .limit(tracks_number)

    @staticmethod
    def enrich_tracks_with_data(tracks_df, data_df):
        """Enrich tracks with extra data(artist, name, etc)

        Args:
            tracks_df:
            data_df:

        Returns:

        """
        tracks_data = data_df \
            .groupBy("traid", 'artid', 'artname', 'traname') \
            .agg(count(col('traid')))

        return tracks_data.join(tracks_df, ["traid"], "leftsemi")


class DataSchema:
    @staticmethod
    def get():
        return StructType() \
            .add("userid", StringType(), True) \
            .add("timestamp", TimestampType(), True) \
            .add("artid", StringType(), True) \
            .add("artname", StringType(), True) \
            .add("traid", StringType(), True) \
            .add("traname", StringType(), True)


class DataLoader:
    """Load data
    """
    def __init__(self, spark):
        self._spark = spark

    def load(self, file_path: AnyStr, schema: DataSchema):
        return self._spark.read.format("csv") \
            .option("header", False) \
            .option("delimiter", "\t") \
            .schema(schema.get()) \
            .load(file_path)
