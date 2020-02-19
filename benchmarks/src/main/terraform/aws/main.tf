##
## Particular Terraform modules
##

module "network" {
  source = "./modules/network"
  aws_access_key = "${var.aws_access_key}"
  aws_secret_key = "${var.aws_secret_key}"
  aws_region = "${var.aws_region}"
  aws_availability_zone = "${var.aws_availability_zone}"
}

module "emr_security" {
  source = "./modules/emr_security"

  aws_access_key = "${var.aws_access_key}"
  aws_secret_key = "${var.aws_secret_key}"
  aws_region = "${var.aws_region}"

  aws_vpc_id = "${module.network.aws_vpc_id}"
  aws_subnet_id = "${module.network.aws_subnet_id}"
}

module "emr_benchmarks_deployment" {
  source = "./modules/emr_benchmarks_deployment"

  aws_access_key = "${var.aws_access_key}"
  aws_secret_key = "${var.aws_secret_key}"
  aws_region = "${var.aws_region}"
  aws_ssh_public_key = "${var.aws_ssh_public_key}"

  aws_vpc_id = "${module.network.aws_vpc_id}"
  aws_subnet_id = "${module.network.aws_subnet_id}"

  aws_core_instance_count = "${var.aws_core_instance_count}"
  aws_instance_type = "${var.aws_instance_type}"
  aws_emr_version = "${var.aws_emr_version}"

  emr_managed_master_security_group_id = "${module.emr_security.emr_managed_master_security_group_id}"
  emr_managed_slave_security_group_id = "${module.emr_security.emr_managed_slave_security_group_id}"
  emr_ec2_instance_profile_arn = "${module.emr_security.emr_ec2_instance_profile_arn}"
  emr_role_arn = "${module.emr_security.emr_role_arn}"

  benchmarks_dataset_specifications_file = "${var.benchmarks_dataset_specifications_file}"
  benchmarks_other_arguments = "${var.benchmarks_other_arguments}"
  benchmarks_driver_memory_gb = "${var.benchmarks_driver_memory_gb}"
  benchmarks_executor_memory_gb = "${var.benchmarks_executor_memory_gb}"
  benchmarks_run_yarn_internal = "${var.benchmarks_run_yarn_internal}"
  benchmarks_run_yarn_external = "${var.benchmarks_run_yarn_external}"
  benchmarks_run_local_internal = "${var.benchmarks_run_local_internal}"

  sw_package_file = "${var.sw_package_file}"
  sw_benchmarks_file = "${var.sw_benchmarks_file}"
  h2o_version_name = "${var.h2o_version_name}"
  h2o_version = "${var.h2o_version}"
  h2o_build = "${var.h2o_build}"
}
