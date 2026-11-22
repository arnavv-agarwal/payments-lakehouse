output "region" {
  value = var.region
}

output "bucket" {
  description = "The lakehouse bucket."
  value       = aws_s3_bucket.lakehouse.bucket
}

output "warehouse" {
  description = "Iceberg warehouse root."
  value       = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse"
}

output "glue_job_role_arn" {
  description = "The role Glue jobs run as."
  value       = aws_iam_role.glue_job.arn
}

output "glue_databases" {
  value = sort(local.glue_db_names)
}

output "glue_jobs" {
  description = "Glue job names."
  value       = sort([for j in aws_glue_job.job : j.name])
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lakehouse.name
}
