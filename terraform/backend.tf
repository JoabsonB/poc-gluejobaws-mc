terraform {
    
    backend "s3" {
        bucket = "jenkins-s3"
        key = "terraform.tfstate"
        region = "us-east-1"
    }

}