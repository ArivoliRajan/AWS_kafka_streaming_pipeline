terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Bucket for data output, checkpoints, JARs, and scripts
resource "aws_s3_bucket" "streaming_bucket" {
  bucket        = var.bucket_name_prefix
  force_destroy = true
}

# Block all public access to the bucket
resource "aws_s3_bucket_public_access_block" "streaming_bucket_public_block" {
  bucket = aws_s3_bucket.streaming_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload JARs from the local 'jars' directory to S3
resource "aws_s3_object" "jars" {
  for_each = fileset("${path.module}/../jars", "*")
  bucket   = aws_s3_bucket.streaming_bucket.id
  key      = "jars/${each.value}"
  source   = "${path.module}/../jars/${each.value}"
  etag     = filemd5("${path.module}/../jars/${each.value}")
}

# Upload the streaming script to S3
resource "aws_s3_object" "pipeline_script" {
  bucket = aws_s3_bucket.streaming_bucket.id
  key    = "scripts/streaming_pipeline.py"
  source = "${path.module}/../streaming_pipeline.py"
  etag   = filemd5("${path.module}/../streaming_pipeline.py")
}

# IAM Role for AWS Glue Interactive Sessions and Jobs
resource "aws_iam_role" "glue_role" {
  name = var.glue_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Attach AWSGlueServiceRole managed policy
resource "aws_iam_role_policy_attachment" "glue_service_attachment" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy to allow AWS Glue full access to the streaming S3 bucket
resource "aws_iam_policy" "glue_s3_policy" {
  name        = "${var.glue_role_name}-s3-policy"
  description = "Allows AWS Glue to read/write/list the streaming S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.streaming_bucket.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.streaming_bucket.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = [
          aws_iam_role.glue_role.arn
        ]
      }
    ]
  })
}

# Attach the custom S3 policy to the Glue IAM Role
resource "aws_iam_role_policy_attachment" "glue_s3_attachment" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

# AWS Glue Streaming Job configuration
resource "aws_glue_job" "streaming_job" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "gluestreaming"
    script_location = "s3://${aws_s3_bucket.streaming_bucket.id}/${aws_s3_object.pipeline_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--class"                            = "GlueApp"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--extra-jars"                       = join(",", [for obj in aws_s3_object.jars : "s3://${aws_s3_bucket.streaming_bucket.id}/${obj.key}"])
  }
}
