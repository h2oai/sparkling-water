module "network" {
  source = "./modules/network"
  aws_access_key = "${var.aws_access_key}"
  aws_secret_key = "${var.aws_secret_key}"
  aws_region = "${var.aws_region}"
  aws_availability_zone = "${var.aws_availability_zone}"
}


module "emr" {
  source = "./modules/emr"

  aws_access_key = "${var.aws_access_key}"
  aws_secret_key = "${var.aws_secret_key}"
  aws_region = "${var.aws_region}"
  aws_ssh_public_key = "${var.aws_ssh_public_key}"

  aws_vpc_id = "${module.network.aws_vpc_id}"
  aws_subnet_id = "${module.network.aws_subnet_id}"

  aws_core_instance_count = "${var.aws_core_instance_count}"
  aws_instance_type = "${var.aws_instance_type}"
  aws_emr_version = "${var.aws_emr_version}"
}