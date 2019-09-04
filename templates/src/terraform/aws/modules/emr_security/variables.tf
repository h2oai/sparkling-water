##
## Input Variables
##
variable "aws_access_key" {}
variable "aws_secret_key" {}
variable "aws_vpc_id" {}
variable "aws_subnet_id" {}
variable "aws_region" {
  default = "us-east-1"
}
