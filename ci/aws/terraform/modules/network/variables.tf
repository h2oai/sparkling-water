##
## Input Variables
##
variable "aws_access_key" {}
variable "aws_secret_key" {}

variable "aws_region" {
  default = "us-west-2"
}
variable "aws_availability_zone" {
  default = "us-west-2b"
}

variable "aws_vpc_cidr_block" {
  default = "10.0.0.0/16"
}
variable "aws_subnet_cidr_block" {
  default = "10.0.0.0/24"
}
