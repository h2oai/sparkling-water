##
## Output variables - used or created resources
##
output "jupyter_notebook_url" {
  value = "${module.emr_deployment.jupyter_notebook_url}"
}
output "master_public_dns" {
  value = "${module.emr_deployment.master_public_dns}"
}
output "bucket" {
  value = "${module.emr_deployment.bucket}"
}