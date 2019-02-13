##
## Input Variables
##
variable "aws_access_key" {}
variable "aws_secret_key" {}

variable "aws_region" {
    default = "us-east-1"    
}
variable "aws_emr_version" {
    default = "emr-5.20.0"
}
variable "aws_core_instance_count" {
    default = "2"
}
variable "aws_instance_type" {
    default = "m3.xlarge"
}
variable "sw_major_version" {
    default = "2.4"
}
variable "sw_patch_version" {
    default = "5"
}

