from delta.tables import *
from AccountDeltaProcessing import AccountDeltaProcessing

table_name = 'account'

if __name__ == "__main__":
    delta = AccountDeltaProcessing(landing_zone_bucket="mastercard-datalake-dbm-landing-zone-dev",
                                  raw_bucket="mastercard-datalake-dbm-raw-zone-dev",
                                  trusted_bucket="mastercard-datalake-dbm-trusted-zone-dev")

    delta.write_to_raw(
        prefix=f"lakehouse/{table_name}",
        format="parquet",
        cols=[*columns])

    delta.write_to_trusted(
        prefix=f"lakehouse/{table_name}",
        sql=query,
        upsert=False)

