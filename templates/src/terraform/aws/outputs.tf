##
## Output variables - used or created resources
##

output "master_public_dns" {
  value = "${module.emr.master_public_dns}"
}