from delta.tables import *
from RedemptionDeltaProcessing import DeltaProcessing

raw_dict = {
    "redemption": ['customerid', 'redemptionid', 'referencenumber', 'rewardmatrixitemid', 'rewardcategoryid',
                   'categoryhierarchyid', 'redemptiondescription', 'creditcode', 'creditredemptionid',
                   'quantity', 'totalpointsredeemed', 'redeemeddate', 'postingdate', 'pointspurchased',
                   'fulfillmentvendorid', 'groupid', 'personalization', 'programid', 'householdid',
                   'emailaddress', 'redemptionsourcecode', 'travelcomponentquantity', 'cashbackamount',
                   'cashbacksentdate', 'redemptionitemdescription', 'externalitemskucode',
                   'externalitemdescription', 'redemptionquantity', 'externalitemid', 'categoryname',
                   'externalemailaddress', 'redeemedhouseholdid1', 'pointsredeemedhh1', 'redeemedhouseholdid2', 'pointsredeemedhh2', 'redeemedhouseholdid3', 'pointsredeemedhh3', 'bankcustomernumber']
}

trusted_dict = {
    "customers": """
                    SELECT c.* ,
                    CAST( GETDATE() AS createdat ),
                    CAST( GETDATE() AS updatedat ),
                    SUBSTRING(redeemeddate, 1, 6) AS redeemedyearmonth
                    FROM customers c
                """
}

if __name__ == "__main__":
    delta = RedemptionDeltaProcessing(landing_zone_bucket="mastercard-datalake-dbm-landing-zone-801772755864",
                                      raw_bucket="mastercard-datalake-dbm-raw-zone-801772755864",
                                      trusted_bucket="mastercard-datalake-dbm-trusted-zone-801772755864")

    for table_name, columns in raw_dict.items():
        delta.write_to_bronze(
            prefix=f"mysql/dataflowmcmysql/{table_name}",
            format="parquet",
            cols=[*columns])

    for table_name, query in trusted_dict.items():
        delta.write_to_silver(
            prefix=f"mysql/dataflowmcmysql/{table_name}",
            sql=query,
            upsert=False)
