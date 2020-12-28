from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (udf, col, concat,
                                   substring, concat_ws, monotonically_increasing_id)
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, FloatType, TimestampType)

#config = configparser.ConfigParser()
#config.read('cap.cfg')

S3_LAND   = 'capstone-pubg/landing'
S3_STAGE  = 'capstone-pubg/staging'

def create_spark_session():
    """
    Create a Spark Session for processing
    """
    spark = SparkSession \
            .builder \
            .getOrCreate()

    return spark

def transform_data(spark, input_path, output_path, hour):
    """
    Load aggregate and kill data from s3 landing_zone(input_path),
    transform them and save it back in s3 staging zone(output_path).

    Params
    ------
    spark : Pyspark Spark Session
        a spark session created by create_spark_session function
    input_path : str
        S3 bucket uri(landing zone)
    output_path : str
        S3 bucket uri(staging zone)
    """

    # aggregate file
    agg_path = "{}/aggregate/sample_agg_aa".format(input_path)
    agg_df = spark.read.csv(agg_path, header=True, inferSchema=True) \
            .dropDuplicates() \
            .dropna()

    # performance dimension table
    agg_df = agg_df.withColumn('performance_id',
                               concat_ws('-',
                                         substring(col('match_id'), 1, 5),
                                         col('player_name')))
    agg_df.createOrReplaceTempView('agg_view_table')

    performance_table = spark.sql(
        """
        SELECT
            performance_id      AS performance_id,
            player_name         AS name,
            match_id            AS match_id,
            player_dbno         AS dbno,
            player_dist_ride    AS dist_ride,
            player_dist_walk    AS dist_walk,
            player_dmg          AS dmg,
            player_kills        AS kills,
            player_survive_time AS survive_time,
            player_assists      AS assists
        FROM
            agg_view_table
        """).dropDuplicates(['performance_id'])
    performance_path = "{}/perf/{}/".format(output_path, hour)
    performance_table.write.parquet(performance_path, mode='overwrite')

    # date dimension table
    date_format = "%Y-%m-%dT%H:%M:%S+%f"
    get_timestamp = udf(lambda date: datetime.strptime(date, date_format),
                        TimestampType())
    agg_df = agg_df.withColumn('game_time', get_timestamp('date'))
    agg_df.createOrReplaceTempView('agg_view_time')
    date_table = spark.sql(
        """
        SELECT
            game_time             AS game_time,
            match_id              AS match_id,
            year(game_time)       AS year,
            month(game_time)      AS month,
            hour(game_time)       AS hour,
            weekofyear(game_time) AS week,
            dayofweek(game_time)  AS weekday
        FROM
            agg_view_time
        """
    ).dropDuplicates(['game_time'])
    date_path = "{}/date/{}/".format(output_path, hour)
    date_table.write.parquet(date_path, mode='overwrite')


    # kill.csv
    kill_path = "{}/kill/sample_kill_aa".format(input_path)
    kill_schema = StructType([
        StructField('killed_by',         StringType()),
        StructField('killer_name',       StringType()),
        StructField('killer_placement',  FloatType()),
        StructField('killer_position_x', FloatType()),
        StructField('killer_position_y', FloatType()),
        StructField('map',               StringType()),
        StructField('match_id',          StringType()),
        StructField('time',              IntegerType()),
        StructField('victim_name',       StringType()),
        StructField('victim_placement',  FloatType()),
        StructField('victim_position_x', FloatType()),
        StructField('victim_position_y', FloatType())
    ])
    kill_df = spark.read.format('csv') \
            .option('header', True) \
            .schema(kill_schema) \
            .load(kill_path) \
            .dropDuplicates() \
            .dropna()
    kill_df.createOrReplaceTempView('kill_view_table')
    agg_df.createOrReplaceTempView('agg_view_table')

    # detail dimension table
    kill_df = kill_df.withColumn('detail_id',
                                 concat(substring(col('match_id'), 1, 5),
                                        col('time'),
                                        col('killer_name')))
    get_int = udf(lambda x: int(x), IntegerType())
    kill_df = kill_df.withColumn('victim_placement',
                                     get_int('victim_placement'))
    kill_df = kill_df.withColumn('killer_placement',
                                     get_int('killer_placement'))
    detail_table = kill_df.select(
        'detail_id',
        'match_id',
        'killer_name',
        'killer_placement',
        'victim_name',
        'victim_placement',
        col('killed_by').alias('weapon'),
        'time'
    ).dropDuplicates(['detail_id'])
    detail_path = "{}/detail/{}/".format(output_path, hour)
    detail_table.write.parquet(detail_path, mode='overwrite')

    # match dimension table
    match_table = spark.sql(
        """
        SELECT
            a.match_id,
            a.game_size,
            a.match_mode,
            k.map
        FROM
            kill_view_table k
        JOIN
            agg_view_table a ON (k.match_id = a.match_id)
        """).dropDuplicates(['match_id'])
    match_path = "{}/match/{}/".format(output_path, hour)
    match_table.write.parquet(match_path, mode='overwrite')

    # location star table
    location_table = kill_df \
            .join(agg_df, kill_df.match_id == agg_df.match_id) \
            .select(
                monotonically_increasing_id().alias('location_id'),
                kill_df.killer_position_x,
                kill_df.killer_position_y,
                kill_df.victim_position_x,
                kill_df.victim_position_y,
                kill_df.match_id,
                kill_df.detail_id,
                agg_df.game_time.alias('date_id'),
                agg_df.performance_id)
    location_path = "{}/location/{}/".format(output_path, hour)
    location_table.write.parquet(location_path, mode='overwrite')


def main():
    spark = create_spark_session()
    input_path  = "s3a://{}/".format(S3_LAND)
    output_path = "s3a://{}/".format(S3_STAGE)
    hour = datetime.now().strftime('%Y%m%d')
    transform_data(spark, input_path, output_path, hour)
    spark.stop()

if __name__ == '__main__':
    main()
