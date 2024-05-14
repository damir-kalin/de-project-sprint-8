from datetime import datetime, timedelta
from time import sleep
import random

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

from get_env import GetEnv

envs = GetEnv()

KAFKA_SECURITY_PROTOCOL = envs.get_env('KAFKA_SECURITY_PROTOCOL')
KAFKA_SASL_MECHANISM = envs.get_env('KAFKA_SASL_MECHANISM')
KAFKA_SERVER = envs.get_env('KAFKA_SERVER')
KAFKA_USER = envs.get_env('KAFKA_USER')
KAFKA_PWD = envs.get_env('KAFKA_PWD')
TOPIC_NAME_IN = envs.get_env('TOPIC_NAME_IN')
USER_ID = 'e4567-e89b-12d3-a456-426614174003'

packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0", "org.postgresql:postgresql:42.4.0"]

restaurants = ['123e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174001']
fio = ['Ivanov Ivan Ivanovich', 'Petrov Petr Petrovich', 'Sidorov Sidor Sidorovich']
mails = ['leon@restaurant.ru', 'sun@restaurant.ru', 'free@restaurant.ru']
contents = ['Action: free burger', 'Action: beer two for the price of one', '50 discount on dumplings']


kafka_security_options = {
    'kafka.security.protocol': KAFKA_SECURITY_PROTOCOL,
    'kafka.sasl.mechanism': KAFKA_SASL_MECHANISM,
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{KAFKA_USER}\" password=\"{KAFKA_PWD}\";'
}

def spark_init(test_name) -> SparkSession:
    return SparkSession.builder\
        .appName(test_name)\
        .master("local")\
        .config("spark.jars.packages", ','.join(packages))\
        .getOrCreate()

def create_dataframe(spark, count_rows):
    users = []
    for _ in range(count_rows):
        user = {}
        user['restaurant_id']= restaurants[random.randint(0,1)]
        user['adv_campaign_id']= str(random.randint(0,1000)).zfill(3) + USER_ID
        user['adv_campaign_content']= contents[random.randint(0,2)]
        user['adv_campaign_owner']= fio[random.randint(0,2)]
        user['adv_campaign_owner_contact']= mails[random.randint(0,2)]
        dt = datetime.now()
        user['adv_campaign_datetime_start']=  int((dt - timedelta(hours=random.randint(0,24))).timestamp())
        user['adv_campaign_datetime_end']=  int((dt + timedelta(hours=random.randint(0,24))).timestamp())
        user['datetime_created']=  int(dt.timestamp())
        users.append(user)
    df = spark.createDataFrame(users)

    df = (df.withColumn('value', f.to_json(
        f.struct(f.col('restaurant_id'),
                 f.col('adv_campaign_id'),
                 f.col('adv_campaign_content'),
                 f.col('adv_campaign_owner'),
                 f.col('adv_campaign_owner_contact'),
                 f.col('adv_campaign_datetime_start'),
                 f.col('adv_campaign_datetime_end'),
                 f.col('datetime_created')
                 )))
            .select(f.col('value')))
    return df
    

def run_query(df):
    df.write\
        .format("kafka")\
        .option('kafka.bootstrap.servers', KAFKA_SERVER)\
        .options(**kafka_security_options)\
        .option("topic", TOPIC_NAME_IN)\
        .option("checkpointLocation", "test_query")\
        .save()


def main():
    spark = spark_init('producer_run')
    while True:
        df = create_dataframe(spark, 10)
        run_query(df)
        sleep(30)
    

if __name__ == '__main__':
    main()