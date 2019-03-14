##
## Output variables - used or created resources
##

output "master_public_dns" {
  value = "${aws_emr_cluster.sparkling-water-cluster.master_public_dns}"
}
output "jupyter_url" {
  value = "https://${aws_emr_cluster.sparkling-water-cluster.master_public_dns}:9443"
}
output "bucket" {
  value = "${format("s3://%s", aws_s3_bucket.sw_bucket.bucket)}"
}
output "jypyter_admin_token" {
  value = "${data.aws_s3_bucket_object.admin_token.body}"
}