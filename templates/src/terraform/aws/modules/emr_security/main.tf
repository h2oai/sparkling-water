##
## Provider Definition
##
provider "aws" {
  region = "${var.aws_region}"
  access_key = "${var.aws_access_key}"
  secret_key = "${var.aws_secret_key}"
}

data "aws_vpc" "main" {
  id = "${var.aws_vpc_id}"
}

data "aws_subnet" "main" {
  id = "${var.aws_subnet_id}"
}
