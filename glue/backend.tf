terraform {
  backend "s3" {
    region = "us-east-1"
    encrypt        = true
  }
}
