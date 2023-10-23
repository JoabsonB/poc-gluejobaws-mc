terraform {
  backend "s3" {
    bucket         = "mastercard-datalake-dbm-jenkins-s3"
    key            = "my-terraform-state-key"
    region         = "us-east-1"
    encrypt        = true
    workspace_key_prefix = "dataflow-mc"
    profile = "joabson.globalmetrics"
  }
}