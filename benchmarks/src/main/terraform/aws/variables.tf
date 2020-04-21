##
## Input Variables
##
variable "aws_access_key" {}
variable "aws_secret_key" {}

variable "aws_ssh_public_key" {
  default = ""
}
variable "aws_region" {
  default = "us-west-2"
}
variable "aws_availability_zone" {
  default = "us-west-2b"
}
variable "aws_emr_version" {
  default = "SUBST_EMR_VERSION"
}
variable "aws_emr_timeout" {
  default = "4 hours"
}
variable "aws_core_instance_count" {
  default = "2"
}
variable "aws_vpc_id" {
  default = ""
}
variable "aws_subnet_id" {
  default = ""
}
variable "aws_instance_type" {
  default = "m5.2xlarge"
}
variable "sw_package_file" {
  default = "SUBST_PACKAGE_FILE"
}
variable "sw_benchmarks_file" {
  default = "SUBST_BENCHMARKS_FILE"
}
variable "h2o_version" {
  default = "SUBST_H2O_VERSION"
}
variable "h2o_version_name" {
  default = "SUBST_H2O_VERSION_NAME"
}
variable "h2o_build" {
  default = "SUBST_H2O_BUILD"
}
variable "benchmarks_dataset_specifications_file" {
  default = "datasets.json"
}
variable "benchmarks_other_arguments" {
  default = ""
}
variable "benchmarks_driver_memory_gb" {
  default = "8"
}
variable "benchmarks_executor_memory_gb" {
  default = "8"
}
variable "benchmarks_run_yarn_internal" {
  default = "true"
}
variable "benchmarks_run_yarn_external" {
  default = "true"
}
variable "benchmarks_run_local_internal" {
  default = "true"
}
