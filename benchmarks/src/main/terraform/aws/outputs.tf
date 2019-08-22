##
## Output variables - used or created resources
##
output "master_public_dns" {
  value = "${module.emr.master_public_dns}"
}
output "bucket" {
  value = "${module.emr.bucket}"
}
output "benchmark_results" {
  value = "${module.emr.benchmark_results}"
}
output "benchmark_finished_file" {
  value = "${module.emr.benchmark_finished_file}"
}
