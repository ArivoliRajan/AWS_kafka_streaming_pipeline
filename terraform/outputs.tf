output "s3_bucket_name" {
  description = "The name of the created S3 bucket"
  value       = aws_s3_bucket.streaming_bucket.id
}

output "glue_role_arn" {
  description = "The ARN of the IAM role created for AWS Glue"
  value       = aws_iam_role.glue_role.arn
}

output "glue_job_name" {
  description = "The name of the created Glue streaming job"
  value       = aws_glue_job.streaming_job.name
}
