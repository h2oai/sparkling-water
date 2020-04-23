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

variable "signing_file" {
  default = ""
}

variable "public_hostname" {
  default = ""
}

variable "github_key_file" {
  default = ""
}

variable "aws_key_file" {
  default = ""
}
