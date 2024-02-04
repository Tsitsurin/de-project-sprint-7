import findspark
findspark.init()
findspark.find()
import pyspark
import pyspark.sql.functions as F 
from pyspark.sql.types import FloatType, DateType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import sys
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

def main():
    date = sys.argv[1]

    def get_spark_session(name=""):
        return SparkSession \
            .builder \
            .master("yarn")\
            .appName(f"{name}") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()

    def get_city(events_geo, geo_city) -> pyspark.sql.DataFrame:

        EARTH_R = 6371

        calculate_diff = 2 * F.lit(EARTH_R) * F.asin(
            F.sqrt(
                F.pow(F.sin((F.radians(F.col("msg_lat")) - F.radians(F.col("city_lat"))) / 2), 2) +
                F.cos(F.radians(F.col("msg_lat"))) * F.cos(F.radians(F.col("city_lat"))) *
                F.pow(F.sin((F.radians(F.col("msg_lon")) - F.radians(F.col("city_lon"))) / 2), 2)
            )
        )

        window = Window().partitionBy('event.message_from').orderBy(F.col('diff').asc())
        events_city = events_geo \
            .crossJoin(geo_city) \
            .withColumn('diff', calculate_diff)\
            .withColumn("row_number", F.row_number().over(window)) \
            .filter(F.col('row_number')==1) \
            .drop('row_number') \
            .persist()

        return events_city

    spark = get_spark_session(name='project_step3')

    date = "2022-05-05" #sys.argv[1]

    path_geo = "/user/elburgan/data/geo_2"
    raw_path_geo_events = "/user/master/data/geo/events"

    events_geo = spark.read.parquet(raw_path_geo_events) \
        .sample(0.05) \
        .where("event_type == 'message'")\
        .withColumn('user_id', F.col('event.message_from'))\
        .withColumnRenamed('lat', 'msg_lat')\
        .withColumnRenamed('lon', 'msg_lon')\
        .withColumn('event_id', F.monotonically_increasing_id())

    geo_city = spark.read.csv(path_geo, sep=';',inferSchema=True, header=True)

    geo_city = geo_city.withColumn('lat', F.regexp_replace('lat', ',', '.')) \
        .withColumn('lat', F.col('lat').cast(FloatType())) \
        .withColumn('lng', F.regexp_replace('lng', ',', '.')) \
        .withColumn('lng', F.col('lng').cast(FloatType())) \
        .withColumnRenamed('lat', "city_lat")\
        .withColumnRenamed('lng', "city_lon")

    events = get_city(
        events_geo=events_geo,
        geo_city=geo_city
    )
    events.show(10)

    w_week = Window.partitionBy(['city', F.trunc(F.col("date"), "week")])
    w_month = Window.partitionBy(['city', F.trunc(F.col("date"), "month")])

    events_zone = events \
        .withColumn("month", F.month(F.col("date")))\
        .withColumn("week", F.weekofyear(F.col("date")))\
        .withColumn('week_message', F.count(F.when(events.event_type == 'message','event_id')).over(w_week))\
        .withColumn('week_reaction', F.count(F.when(events.event_type == 'reaction','event_id')).over(w_week)) \
        .withColumn('week_subscription', F.count(F.when(events.event_type == 'subscription','event_id')).over(w_week)) \
        .withColumn('week_user', F.count(F.when(events.event_type == 'registration','event_id')).over(w_week)) \
        .withColumn('month_message', F.count(F.when(events.event_type == 'message','event_id')).over(w_month)) \
        .withColumn('month_reaction', F.count(F.when(events.event_type == 'reaction','event_id')).over(w_month)) \
        .withColumn('month_subscription', F.count(F.when(events.event_type == 'subscription','event_id')).over(w_month)) \
        .withColumn('month_user', F.count(F.when(events.event_type == 'registration','event_id')).over(w_month))\
        .selectExpr("month", "week", "id as zone_id","week_message","week_reaction","week_subscription","week_user","month_message","month_reaction","month_subscription","month_user")\
        .distinct()

    events_zone.write.mode('overwrite').parquet(f'/user/elburgan/analitics/events_zone/date={date}')

if __name__ == "__main__":
    main()