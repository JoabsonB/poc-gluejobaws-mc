from delta.tables import *
from RedemptionDeltaProcessing import RedemptionDeltaProcessing

table_name = 'redemption'

if __name__ == "__main__":
    delta = RedemptionDeltaProcessing(landing_zone_bucket="mastercard-datalake-dbm-landing-zone-dev",
                            raw_bucket="mastercard-datalake-dbm-raw-zone-dev",
                            trusted_bucket="mastercard-datalake-dbm-trusted-zone-dev")

    delta.write_to_raw(
        prefix=f"lakehouse/{table_name}",
        format="csv")

    delta.write_to_trusted(
        prefix=f"lakehouse/{table_name}",
        upsert=False )
