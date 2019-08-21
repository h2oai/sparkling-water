##
## Output variables - used or created resources
##
output "master_public_dns" {
  value = "${aws_emr_cluster.sparkling-water-cluster.master_public_dns}"
}
output "bucket" {
  value = "${format("s3://%s", aws_s3_bucket.deployment_bucket.bucket)}"
}
output "benchmark_results" {
  value = "${format("https://s3-us-west-2.amazonaws.com/%s/public-read/results", aws_s3_bucket.deployment_bucket.bucket)}"
}
output "benchmark_finished_file" {
  value = "${format("https://s3-us-west-2.amazonaws.com/%s/public-read/finished", aws_s3_bucket.deployment_bucket.bucket)}"
}