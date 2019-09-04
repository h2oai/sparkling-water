##
## Output variables - used or created resources
##
output "master_public_dns" {
  value = "${module.emr_benchmarks_deployment.master_public_dns}"
}
output "bucket" {
  value = "${module.emr_benchmarks_deployment.bucket}"
}
output "benchmark_results" {
  value = "${module.emr_benchmarks_deployment.benchmark_results}"
}
output "benchmark_finished_file" {
  value = "${module.emr_benchmarks_deployment.benchmark_finished_file}"
}
