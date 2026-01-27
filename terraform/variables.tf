variable "aws_region" {
  description = "AWS region"
  type = string
  default = "us-east-1"

}

variable "bucket_name" {
  description = "S3 bucket"
  type        = string
  default     = ""
}


variable "dataset_name" {
  description = "Glue Catalog"
  type        = string
  default = "ny_taxi_database"
}