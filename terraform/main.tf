terraform {
    required_providers {
        aws = {
            source  = "hashicorp/aws"
            version = "~> 5.0"
        }
    }
}

provider "aws" {
    region = var.aws_region
}

resource "aws_s3_bucket" "data_lake_bucket" {
  bucket        = var.bucket_name
  force_destroy = true
}


resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake_bucket.id 

  versioning_configuration {
    status = "Enabled" 
  }
}

resource "aws_s3_bucket_public_access_block" "block_public_access" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lifecycle_rules" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  rule {
    id     = "Delete_old_older_than_30_days"
    status = "Enabled"

    expiration {
      days = 30
    }
    filter {
      prefix = "" 
    }
  }
}

resource "aws_glue_catalog_database" "dataset" {
  name = var.dataset_name
}