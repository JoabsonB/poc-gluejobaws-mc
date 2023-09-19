resource "aws_s3_object" "jars" {
  bucket = "mastercard-datalake-dbm-scripts-dev"
  key    = "jars/delta-core_2.12-1.0.0.jar"
  source = "../jars/delta-core_2.12-1.0.0.jar"
  etag   = filemd5("../jars/delta-core_2.12-1.0.0.jar")
}

resource "aws_s3_object" "class" {
  bucket = "mastercard-datalake-dbm-scripts-dev"
  key    = "class/RedemptionDeltaProcessing.py"
  source = "../class/RedemptionDeltaProcessing.py"
  etag   = filemd5("../class/RedemptionDeltaProcessing.py")
}

resource "aws_s3_object" "etl" {
  bucket = "mastercard-datalake-dbm-scripts-dev"
  key    = "job/etl_redemption.py"
  source = "../job/etl_redemption.py"
  etag   = filemd5("../job/etl_redemption.py")
}
