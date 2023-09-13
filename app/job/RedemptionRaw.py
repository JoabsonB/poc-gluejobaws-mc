from pyspark.sql import SparkSession


class RedemptionRawProcessing:
    def __init__(
        self,
        landing_zone_bucket: str = None,
        raw_bucket: str = None,
        silver_bucket: str = None,
        gold_bucket: str = None,
        spark=SparkSession.builder.getOrCreate()
                ):

        self.spark = spark
        self.landing_zone_bucket = landing_zone_bucket
        self.raw_bucket = raw_bucket
        self.silver_bucket = silver_bucket
        self.gold_bucket = gold_bucket


    def create_spark_session():
        spark = SparkSession.builder.getOrCreate()
        return spark

    def write_to_raw(
            self,
        prefix: str,
        format: str,
        cols: list
        ):

        df = self.spark.read.format(format).load(
        f"s3a://{self.landing_zone_bucket}/{prefix}")

        df.select(*cols).write.mode('overwrite').format(
            "delta").save(f"s3a://{self.raw_bucket}/{prefix}")

        deltaTable = DeltaTable.forPath(
            self.spark, f"s3a://{self.raw_bucket}/{prefix}")
        deltaTable.generate("symlink_format_manifest")





