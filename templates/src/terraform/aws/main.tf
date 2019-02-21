module "network" {
  source = "modules/network"
  aws_access_key = "${var.aws_access_key}"
  aws_secret_key = "${var.aws_secret_key}"
  aws_region = "${var.aws_region}"
}


module "emr" {
  source = "modules/emr"

  aws_access_key = "${var.aws_access_key}"
  aws_secret_key = "${var.aws_secret_key}"
  aws_region = "${var.aws_region}"
  aws_ssh_public_key = "${var.aws_ssh_public_key}"

  aws_vpc_id = "${module.network.aws_vpc_id}"
  aws_subnet_id = "${module.network.aws_subnet_id}"
}
