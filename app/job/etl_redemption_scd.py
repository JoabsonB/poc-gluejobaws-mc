from delta.tables import *
from RedemptionDeltaProcessing import RedemptionDeltaProcessing

table_name = 'redemption'

if __name__ == "__main__":
    delta = RedemptionDeltaProcessing(landing_zone_bucket="mastercard-datalake-dbm-landing-zone",
                                      raw_bucket="mastercard-datalake-dbm-raw-zone")

    delta.write_to_raw(
        prefix=f"lakehouse/{table_name}",
        format="csv"
    )
