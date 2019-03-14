##
## Output variables - used or created resources
##

output "master_public_dns" {
  value = "${module.emr.master_public_dns}"
}
output "jupyter_url" {
  value = "${module.emr.jupyter_url}"
}
output "bucket" {
  value = "${module.emr.bucket}"
}
output "jypyter_admin_token" {
  value = "${module.emr.jypyter_admin_token}"
}