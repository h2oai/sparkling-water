##
## Input Variables
##
variable "aws_access_key" {}
variable "aws_secret_key" {}

variable "aws_ssh_public_key" {}
variable "aws_vpc_id" {}
variable "aws_subnet_id" {}
variable "aws_region" {
  default = "us-east-1"
}
variable "aws_emr_version" {
  default = "emr-5.21.0"
}
variable "aws_core_instance_count" {
  default = "2"
}
variable "aws_instance_type" {
  default = "m3.xlarge"
}
variable "sw_major_version" {
  default = "SUBST_MAJOR_VERSION"
}
variable "sw_patch_version" {
  default = "SUBST_MINOR_VERSION"
}
variable "jupyter_name" {
  default = "admin"
}
variable "jupyter_pass" {
  default = "admin"
}
