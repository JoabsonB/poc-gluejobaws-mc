resource "aws_glue_job" "redemption_glue_job" {
  name              = "glue_script_redemption"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "3.0"
  worker_type       = "Standard"
  number_of_workers = 2
  timeout           = 10

  command {
    script_location = "s3://${local.glue_bucket}/job/etl_redemption.py"
    python_version  = "3"
  }

  default_arguments = {
    "--additional-python-modules" = "delta-spark==1.0.0"
    "--extra-jars" = "mastercard-datalake-dbm-glue-scripts/jars/delta-core_2.12-1.0.0.jar"
    "--conf spark.delta.logStore.class" = "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
    "--conf spark.sql.extensions" = "io.delta.sql.DeltaSparkSessionExtension",
    "--extra-py-files" = "s3://mastercard-datalake-dbm-dev-scripts/class/RedemptionDeltaProcessing.py"
  }
}