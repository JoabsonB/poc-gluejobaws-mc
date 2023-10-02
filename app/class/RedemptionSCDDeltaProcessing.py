from pyspark.sql import SparkSession
from delta.tables import *
from pyspark.sql import types as T
from pyspark.sql import functions as F
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

        df = df.toDF('customerid',
                     'redemptionid',
                     'referencenumber',
                     'rewardmatrixitemid',
                     'rewardcategoryid',
                     'categoryhierarchyid',
                     'redemptiondescription',
                     'creditcode',
                     'creditredemptionid',
                     'quantity',
                     'totalpointsredeemed',
                     'redeemeddate',
                     'postingdate',
                     'pointspurchased',
                     'fulfillmentvendorid',
                     'groupid',
                     'personalization',
                     'programid',
                     'householdid',
                     'emailaddress',
                     'redemptionsourcecode',
                     'travelcomponentquantity',
                     'cashbackamount',
                     'cashbacksentdate',
                     'redemptionitemdescription',
                     'externalitemskucode',
                     'externalitemdescription',
                     'redemptionquantity',
                     'externalitemid',
                     'categoryname',
                     'externalemailaddress',
                     'redeemedhouseholdid1',
                     'pointsredeemedhh1',
                     'redeemedhouseholdid2',
                     'pointsredeemedhh2',
                     'redeemedhouseholdid3',
                     'pointsredeemedhh3',
                     'bankcustomernumber')
        
        df = (
            df.select("*",
                      F.lit(dt.datetime.now()).alias("created_at"),
                      F.lit(dt.datetime.now()).alias("valid_to"),
                      F.lit(df.redeemeddate[1:6]).alias("redeemed_year_month"),
                      F.lit(F.split(F.input_file_name(), '/').getItem(5)).alias("filename")
                      ))
        
        df.coalesce(1).write.format('delta').mode('overwrite').save(
            f"s3a://{self.raw_bucket}/{prefix}/delta")#.saveAsTable('redemption_raw')

        df.createOrReplaceTempView('redemption_raw')

        scd_df = (
            df
            .select("*",
                    F.lit('').cast("timestamp").alias("valid_from"),
                    F.lit(True).cast("boolean").alias("current")
                    ))

        scd_df.coalesce(1).write.format('delta').mode('overwrite').save(
        f"s3a://{self.raw_bucket}/{prefix}/current")#.saveAsTable('redemption_scd2')

        scd_df.createOrReplaceTempView('redemption_scd')


        scd_df = DeltaTable.forPath(
            self.spark, f"s3a://{self.raw_bucket}/{prefix}/current")
            

        sqlContext.sql(
        """      
        MERGE INTO redemption_scd AS scd
            USING
            (
            SELECT cc.redemptionid AS mergerkey, 
            cc.*,
            '' as valid_from,
            True as current
            FROM redemption_raw cc
        UNION ALL
            SELECT NULL as mergerkey, 
            cc.customerid,
            cc.redemptionid,
            cc.referencenumber,
            cc.rewardmatrixitemid,
            cc.rewardcategoryid,
            cc.categoryhierarchyid,
            cc.redemptiondescription,
            cc.creditcode,
            cc.creditredemptionid,
            cc.quantity,
            cc.totalpointsredeemed,
            cc.redeemeddate,
            cc.postingdate,
            cc.pointspurchased,
            cc.fulfillmentvendorid,
            cc.groupid,
            cc.personalization,
            cc.programid,
            cc.householdid,
            cc.emailaddress,
            cc.redemptionsourcecode,
            cc.travelcomponentquantity,
            cc.cashbackamount,
            cc.cashbacksentdate,
            cc.redemptionitemdescription,
            cc.externalitemskucode,
            cc.externalitemdescription,
            cc.redemptionquantity,
            cc.externalitemid,
            cc.categoryname,
            cc.externalemailaddress,
            cc.redeemedhouseholdid1,
            cc.pointsredeemedhh1,
            cc.redeemedhouseholdid2,
            cc.pointsredeemedhh2,
            cc.redeemedhouseholdid3,
            cc.pointsredeemedhh3,
            cc.bankcustomernumber,
            cc.created_at,
            cc.valid_to,
            cc.redeemed_year_month,
            cc.filename,
            '' as valid_from,
            True as current
        FROM redemption_raw cc
            ) ud
        ON scd.redemptionid = ud.mergerkey AND scd.current is True  
        WHEN MATCHED 
            THEN UPDATE SET 
                scd.valid_from = ud.valid_to,
            scd.current  = False                        
        WHEN NOT MATCHED 
        AND ud.mergerkey is null 
        THEN INSERT *
        """
        )

        df.write.mode('overwrite').format("delta").save(
            f"s3a://{self.raw_bucket}/{prefix}/processed")

        deltaTable = DeltaTable.forPath(
            self.spark, f"s3a://{self.raw_bucket}/{prefix}")
        deltaTable.generate("symlink_format_manifest")


