from pyspark.sql import SparkSession
from delta.tables import *
from pyspark.sql.functions import lit, input_file_name
import datetime as dt


class RedemptionDeltaProcessing:
    def __init__(
        self,
        landing_zone_bucket: str = None,
        raw_bucket: str = None,
        spark=SparkSession.builder.getOrCreate()
    ):

        self.spark = spark
        self.landing_zone_bucket = landing_zone_bucket
        self.raw_bucket = raw_bucket

    def write_to_raw(
        self,
        prefix: str,
        format: str
    ):

        df = self.spark.read.format(format).option('header', 'true').option('sep', '|').load(
            f"s3a://{self.landing_zone_bucket}/{prefix}")

        df = df.toDF('customerid', 'redemptionid', 'referencenumber', 'rewardmatrixitemid', 'rewardcategoryid', 'categoryhierarchyid', 'redemptiondescription', 'creditcode', 'creditredemptionid', 'quantity', 'totalpointsredeemed', 'redeemeddate', 'postingdate', 'pointspurchased', 'fulfillmentvendorid', 'groupid', 'personalization', 'programid', 'householdid', 'emailaddress', 'redemptionsourcecode', 'travelcomponentquantity',
                     'cashbackamount', 'cashbacksentdate', 'redemptionitemdescription', 'externalitemskucode', 'externalitemdescription', 'redemptionquantity', 'externalitemid', 'categoryname', 'externalemailaddress', 'redeemedhouseholdid1', 'pointsredeemedhh1', 'redeemedhouseholdid2', 'pointsredeemedhh2', 'redeemedhouseholdid3', 'pointsredeemedhh3', 'bankcustomernumber')
        
        df = (
            df.select("*",)
            .withColumn('created_at', lit(dt.datetime.now()))
            .withColumn('valid_from', lit(df.redeemeddate))
            .withColumn('redeemed_year_month', df.redeemeddate[1:6])
            .withColumn('filename', input_file_name())
        )
        
        df.coalesce(1).write.format('delta').mode('overwrite').save(
            f"s3a://{self.raw_bucket}/{prefix}/current")#.saveAsTable('redemption_raw')

        df.createOrReplaceTempView('redemption_raw')

        scd_df = (
            df.select("*",)
            .withColumn('valid_to', lit(''))
            #.withColumn('version_nr', lit('1'))
            .withColumn('current_version', lit(True))
        )

        scd_df.coalesce(1).write.format('delta').mode('overwrite').save(
        f"s3a://{self.raw_bucket}/{prefix}/delta")#.saveAsTable('redemption_scd2')

        scd_df.createOrReplaceTempView('redemption_scd2')


        scd_df = DeltaTable.forPath(
            self.spark, f"s3a://{self.raw_bucket}/{prefix}/delta")
            
        df = DeltaTable.forPath(
            self.spark, f"s3a://{self.raw_bucket}/{prefix}/current")

        # Rows to INSERT new 'redeemeddate' of existing redemption
        newRedemptionToInsert = df \
            .alias("s") \
            .join(scd_df.toDF().alias("d"), "redemptionid") \
            .where("s.redeemeddate <> d.redeemeddate AND d.current_version = 'false'")  # Qual a coluna de comparação se houver modificação, estou usando a redeemeddate
            
        # Stage the update by unioning two sets of rows
        # 1. Rows that will be inserted in the whenNotMatched clause
        # 2. Rows that will either update the current redeemeddate of existing redemption or insert the new redeemeddate of new redemption
        scddUpdates = (
            newRedemptionToInsert
            .selectExpr("NULL as mergeKey", "s.*")   # Rows for 1
            # Rows for 2.
            .union(df.selectExpr("redemptionid as mergeKey", "*"))
        )

        # Apply SCD Type 2 operation using merge


        scd_df.alias("t").merge(
            scddUpdates.alias("s"),
            "s.redemptionid = t.redemptionid") \
            .whenMatchedUpdate(
                condition="s.current_version = true AND s.redeemeddate <> d.redeemeddate",
                set={
                    "current_version": "false",
                    "valid_to": "d.valid_from"
            }) \
            .whenNotMatchedInsert(
            values={
                "redemptionid": "d.redemptionid",
                "redeemeddate": "d.redeemeddate",
                "current": "true",
                "valid_from": "d.valid_from",
                "endDate": "null"
            }
        ).execute()

        df.write.mode('overwrite').format("delta").save(
            f"s3a://{self.raw_bucket}/{prefix}/processed")

        deltaTable = DeltaTable.forPath(
            self.spark, f"s3a://{self.raw_bucket}/{prefix}")
        deltaTable.generate("symlink_format_manifest")
