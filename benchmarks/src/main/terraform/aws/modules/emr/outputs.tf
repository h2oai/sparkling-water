##
## Output variables - used or created resources
##
output "master_public_dns" {
  value = "${aws_emr_cluster.sparkling-water-cluster.master_public_dns}"
}
output "bucket" {
  value = "${format("s3://%s", aws_s3_bucket.sw_bucket.bucket)}"
}
