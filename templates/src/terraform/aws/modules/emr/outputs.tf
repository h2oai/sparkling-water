##
## Output variables - used or created resources
##
output "jupyter_notebook_url" {
  value = "https://${aws_emr_cluster.sparkling-water-cluster.master_public_dns}:9443/user/${var.jupyter_name}/tree?token=${data.aws_s3_bucket_object.user_token.body}"
}
output "master_public_dns" {
  value = "${aws_emr_cluster.sparkling-water-cluster.master_public_dns}"
}
output "bucket" {
  value = "${format("s3://%s", aws_s3_bucket.sw_bucket.bucket)}"
}