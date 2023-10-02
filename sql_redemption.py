
# Retorna os casos onde o redemptionid se repete 
sqlContext.sql(
"""

select redemptionid, count(*) from redemption_raw
group by redemptionid

""" )

# Uso como exemplo o redemptionid = '0773893301'
sqlContext.sql(
"""

select * from redemption_raw
where redemptionid = '0773893301'

""")

# Teste com SQL - Testando um redemptionid com merge 0773893301
sqlContext.sql(
"""

SELECT
*
FROM redemption_scd scd
JOIN(SELECT cc.redemptionid AS mergerkey,
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
     FROM redemption_raw cc) ud
ON scd.redemptionid = ud.mergerkey AND scd.current is True
WHERE scd.redemptionid = '0773893301'

""")
