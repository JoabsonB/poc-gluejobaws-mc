from delta.tables import *
from DeltaProcessing import RedemptionBronze, RedemptionRaw, RedemptionTrusted, RedemptionRefined

raw_dict = {
    "redemption": ['customerid', 'redemptionid', 'referencenumber', 'rewardmatrixitemid', 'rewardcategoryid', 'categoryhierarchyid', 'redemptiondescription', 'creditcode', 'creditredemptionid', 'quantity', 'totalpointsredeemed', 'redeemeddate', 'postingdate', 'pointspurchased', 'fulfillmentvendorid', 'groupid', 'personalization', 'programid', 'householdid', 'emailaddress', 'redemptionsourcecode',
                   'travelcomponentquantity', 'cashbackamount', 'cashbacksentdate', 'redemptionitemdescription', 'externalitemskucode', 'externalitemdescription', 'redemptionquantity', 'externalitemid', 'categoryname', 'externalemailaddress', 'redeemedhouseholdid1', 'pointsredeemedhh1', 'redeemedhouseholdid2', 'pointsredeemedhh2', 'redeemedhouseholdid3', 'pointsredeemedhh3', 'bankcustomernumber']
}



if __name__ == "__main__":
    delta = RedemptionRaw(landing_zone_bucket="mastercard-datalake-dbm-landing-zone-dev",
                             raw_bucket="mastercard-datalake-raw-zone-layer-dev"
                             trusted_bucket="mastercard-datalake-trusted-zone-layer-dev"
                             refined_bucket="mastercard-datalake-refined-zone-layer-dev"
                             )

    for table_name, columns in raw_dict.items():
        delta.write_to_raw(
            prefix=f"mysql/mastercardmysql/{table_name}",
            format="parquet",
            cols=[*columns])

    for table_name, query in trusted_dict.items():
        delta.write_to_silver(
            prefix=f"mysql/gmreceitamysql/{table_name}",
            sql=query,
            upsert=False)

    for table_name, query in refined_dict.items():
        delta.write_to_refined(
            prefix_list=refined_list,
            prefix=f"delivery/{table_name}",
            sql=query,
            upsert=False)
