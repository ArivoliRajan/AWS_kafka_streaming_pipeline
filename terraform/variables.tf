variable "aws_region" {
  description = "The AWS region to provision resources in"
  type        = string
  default     = "us-west-1"
}

variable "bucket_name_prefix" {
  description = "Prefix for the S3 bucket to create"
  type        = string
  default     = "orders-realtime-bucket"
}

variable "glue_role_name" {
  description = "Name of the IAM role for the Glue Interactive Sessions and Jobs"
  type        = string
  default     = "GlueStreamingPipelineRole"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue Streaming Job"
  type        = string
  default     = "kafka_streaming_pipeline"
}
