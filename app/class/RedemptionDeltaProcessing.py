from pyspark.sql import SparkSession
from delta.tables import *
from pyspark.sql.functions import lit
import datetime as dt

class RedemptionDeltaProcessing:
    def __init__(
        self,
        landing_zone_bucket: str = None,
        raw_bucket: str = None,
        trusted_bucket: str = None,
        spark=SparkSession.builder.getOrCreate()
    ):

        self.spark = spark
        self.landing_zone_bucket = landing_zone_bucket
        self.raw_bucket = raw_bucket
        self.trusted_bucket = trusted_bucket

    def write_to_raw(
        self,
        prefix: str,
        format: str,
    ):

        df = self.spark.read.format(format).option('header', 'true').option('sep', '|').load(
            f"s3a://{self.landing_zone_bucket}/{prefix}")

        df.write.mode('overwrite').format(
            "delta").save(f"s3a://{self.raw_bucket}/{prefix}")

        deltaTable = DeltaTable.forPath(
            self.spark, f"s3a://{self.raw_bucket}/{prefix}")
        deltaTable.generate("symlink_format_manifest")

    def write_to_raw(
        self,
        prefix: str,
        ##sql: str,
        upsert: bool,
        comparative_keys: str = None,
        insert_condition: str = None,
        update_condition: str = None,
        delete_condition: str = None,
    ):

        df = self.spark.read.load(f"s3a://{self.raw_bucket}/{prefix}")
        table = prefix.split("/")[-1]

        df = df.withColumn('createdat', lit(dt.datetime.now()))\
               .withColumn('updatedat', lit(dt.datetime.now()))\
               .withColumn('redeemedyearmonth', df.redeemeddate[1:6])
        
        df.createOrReplaceTempView(table)
        ##df = self.spark.sql(sql) ##Add transform for spark not sql

        if upsert:
            silver_data = DeltaTable.forPath(
                self.spark, f"s3a://{self.trusted_bucket}/{prefix}")

            if delete_condition:
                print(f"Proceeding with delete condition = {delete_condition}")
                delete_condition = upsert["delete"]
                (
                    silver_data.alias("s")
                    .merge(df.alias("d"), comparative_keys)
                    .whenMatchedDelete(condition=delete_condition)
                    .whenMatchedUpdateAll(condition=update_condition)
                    .whenNotMatchedInsertAll(condition=insert_condition)
                    .execute()
                )

            else:
                print("Processing with no delete condition")

                (
                    silver_data.alias("s")
                    .merge(df.alias("d"), comparative_keys)
                    .whenMatchedUpdateAll(condition=update_condition)
                    .whenNotMatchedInsertAll(condition=insert_condition)
                    .execute()
                )
        else:
            df.write.mode('overwrite').format("delta").save(
                f"s3a://{self.trusted_bucket}/{prefix}")

            deltaTable = DeltaTable.forPath(
                self.spark, f"s3a://{self.trusted_bucket}/{prefix}")
            deltaTable.generate("symlink_format_manifest")






