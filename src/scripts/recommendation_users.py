import findspark
findspark.init()
findspark.find()
import pyspark
import pyspark.sql.functions as F 
from pyspark.sql.functions import radians, cos, sin, asin, sqrt
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

def df_local_time(events: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return events.withColumn("TIME",F.col("event.datetime").cast("Timestamp"))\
        .withColumn("TIME",F.col("event.datetime").cast("Timestamp"))\
        .withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))\
        .select("TIME", "local_time", 'city', 'user_id')

def haversine(lat1, lon1, lat2, lon2):
    """
    Рассчитывает расстояние между двумя точками по координатам (широта и долгота) с использованием формулы haversine.
    """
    R = 6371  # Радиус Земли в километрах

    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)

    a = (
        (F.sin(dlat / 2) ** 2) +
        F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * (F.sin(dlon / 2) ** 2)
    )

    c = 2 * F.atan2(sqrt(a), F.sqrt(1 - a))

    distance = R * c

    return distance

spark = get_spark_session(name='project_step4')

date = "2022-05-05" #sys.argv[1]

path_geo = "/user/elburgan/data/geo_2"
raw_path_geo_events = "/user/master/data/geo/events"

events_geo = spark.read.parquet(raw_path_geo_events) \
    .sample(0.01) \
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

w_l = Window.partitionBy('user_id').orderBy(F.col('event.message_ts').desc())
view_last = events.where('msg_lon is not null') \
    .withColumn("rn",F.row_number().over(Window().partitionBy('user_id').orderBy(F.col('event.message_ts').desc()))) \
    .filter(F.col('rn') == 1) \
    .drop(F.col('rn')) \
    .selectExpr('user_id', 'msg_lon as lon', 'msg_lat as lat')

# локальное время сразу к событиям получил и дальше уже с ним выборки делаю    
events_with_local_time = df_local_time(events)
events_city = events_with_local_time.select('user_id', 'city', 'local_time')

view_last = view_last.join(events_city, ['user_id'], 'inner')

view_last_channel = events.select(
    F.col('event.subscription_channel').alias('channel'),
    F.col('event.user').alias('user_id')
).distinct()

new = view_last_channel.join(view_last_channel.withColumnRenamed('user_id', 'user_id2'), ['channel'], 'inner') \
    .filter('user_id < user_id2')
    
user_list = new.join(view_last,['user_id'],'inner') \
    .withColumnRenamed('lon','lon_user1') \
    .withColumnRenamed('lat','lat_user1') #\
    #.drop('city').drop('local_time')

user_list = user_list\
    .join(view_last, view_last['user_id'] == user_list['user_id2'], 'inner').drop(view_last['user_id']) \
    .withColumnRenamed('lon','lon_user2') \
    .withColumnRenamed('lat','lat_user2') #\
    #.drop(view_last['city'])

user_list_distance = user_list\
    .withColumn("distance", haversine(F.col("lat_user1"), F.col("lon_user1"), F.col("lat_user2"), F.col("lon_user2")))\
    .filter(F.col("distance") <= 1.0)

user_list_distinc = user_list_distance.select("user_id", "user_id2").distinct()

user_convers_1 = events.selectExpr("event.message_to as user_id", "event.message_from as user_id2").distinct()

user_convers_2 = events.selectExpr("event.message_from as user_id", "event.message_to as user_id2").distinct()

all_user_convers = user_convers_1.union(user_convers_2).distinct()

result_user_convers = all_user_convers.subtract(user_list_distinc)

final_user_list = user_list_distance.alias("uld")\
    .join(result_user_convers.selectExpr("user_id as ruc_user_id", "user_id2 as ruc_user_id2"), 
        on=[F.col("uld.user_id") == F.col("ruc_user_id"),
            F.col("uld.user_id2") == F.col("ruc_user_id2")],
        how="inner")\
    .select(
        F.col("uld.user_id").alias("user_left"),
        F.col("uld.user_id2").alias("user_right"),
        F.col("uld.city").alias("zone"),
        F.col("uld.local_time")
    )\
    .withColumn("processed_dttm", F.current_timestamp())

final_user_list.write.mode('overwrite').parquet(f'/user/elburgan/analitics/recommendation_users/date={date}')

if __name__ == "__main__":
    main()