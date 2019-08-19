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
variable "aws_core_instance_count" {
  default = "2"
}
variable "aws_instance_type" {
  default = "m3.xlarge"
}
variable "sw_package_file" {
  default = "SUBST_PACKAGE_FILE"
}
