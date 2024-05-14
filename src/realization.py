from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType

from get_env import GetEnv

envs = GetEnv()

PG_HOST = envs.get_env('PG_HOST')
PG_PORT = envs.get_env('PG_PORT')
PG_DB = envs.get_env('PG_DB')
PG_USER = envs.get_env('PG_USER')
PG_PWD = envs.get_env('PG_PWD')
KAFKA_SECURITY_PROTOCOL = envs.get_env('KAFKA_SECURITY_PROTOCOL')
KAFKA_SASL_MECHANISM = envs.get_env('KAFKA_SASL_MECHANISM')
KAFKA_SERVER = envs.get_env('KAFKA_SERVER')
KAFKA_USER = envs.get_env('KAFKA_USER')
KAFKA_PWD = envs.get_env('KAFKA_PWD')
TOPIC_NAME_IN = envs.get_env('TOPIC_NAME_IN')
TOPIC_NAME_OUT = envs.get_env('TOPIC_NAME_OUT')

kafka_security_options = {
    'kafka.security.protocol': KAFKA_SECURITY_PROTOCOL,
    'kafka.sasl.mechanism': KAFKA_SASL_MECHANISM,
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{KAFKA_USER}\" password=\"{KAFKA_PWD}\";'
}

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df.write \
        .format("jdbc") \
        .option('driver', 'org.postgresql.Driver') \
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
        .option("dbtable", "public.subscribers_feedback") \
        .option("user", PG_USER) \
        .option("password", PG_PWD) \
        .mode("append") \
        .save()
    # создаём df для отправки в Kafka. Сериализация в json.
    df = (df.withColumn('value', to_json(
        struct(col('restaurant_id'),
                col('adv_campaign_id'),
                col('adv_campaign_content'),
                col('adv_campaign_owner'),
                col('adv_campaign_owner_contact'),
                col('adv_campaign_datetime_start'),
                col('adv_campaign_datetime_end'),
                col('client_id'),
                col('datetime_created'),
                col('trigger_datetime_created')
                 )))
            .select(col('value')))
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df.write\
        .format("kafka")\
        .option('kafka.bootstrap.servers', KAFKA_SERVER) \
        .options(**kafka_security_options)\
        .option("topic", TOPIC_NAME_OUT)\
        .save()
    # очищаем память от df
    df.unpersist()

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .master('local') \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_SERVER) \
    .options(**kafka_security_options) \
    .option('subscribe', TOPIC_NAME_IN) \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True)
])

restaurant_read_stream_df = (restaurant_read_stream_df
    .withColumn('value', col('value').cast(StringType()))
    .withColumn('key', col('key').cast(StringType()))
    .withColumn('event', from_json(col('value'), incomming_message_schema))
    .selectExpr('event.*', '*')
    .select(
        col('restaurant_id'),
        col('adv_campaign_id'),
        col('adv_campaign_content'),
        col('adv_campaign_owner'),
        col('adv_campaign_owner_contact'),
        col('adv_campaign_datetime_start'),
        col('adv_campaign_datetime_end'),
        col('datetime_created')
    )
)


# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
dt = datetime.now()
current_timestamp_utc = int(dt.timestamp())

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (restaurant_read_stream_df
    .filter(lit(current_timestamp_utc).between(col('adv_campaign_datetime_start'), col('adv_campaign_datetime_end')))
)

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', f'jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', PG_USER) \
                    .option('password', PG_PWD) \
                    .load()

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = (restaurant_read_stream_df
    .join(subscribers_restaurant_df, 'restaurant_id', 'inner')
    .withColumn('trigger_datetime_created', lit(current_timestamp_utc))
    .select(
        col('restaurant_id'),
        col('adv_campaign_id'),
        col('adv_campaign_content'),
        col('adv_campaign_owner'),
        col('adv_campaign_owner_contact'),
        col('adv_campaign_datetime_start'),
        col('adv_campaign_datetime_end'),
        col('client_id'),
        col('datetime_created'),
        col('trigger_datetime_created')
    )
)

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination() 
