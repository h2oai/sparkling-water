##
## Particular EMR modules
##
module "emr_security" {
  source = "../emr_security"

  aws_access_key = "${var.aws_access_key}"
  aws_secret_key = "${var.aws_secret_key}"
  aws_region = "${var.aws_region}"

  aws_vpc_id = "${var.aws_vpc_id}"
  aws_subnet_id = "${var.aws_subnet_id}"
}

module "emr_deployment" {
  source = "../emr_deployment"

  aws_access_key = "${var.aws_access_key}"
  aws_secret_key = "${var.aws_secret_key}"
  aws_region = "${var.aws_region}"
  aws_ssh_public_key = "${var.aws_ssh_public_key}"

  aws_vpc_id = "${var.aws_vpc_id}"
  aws_subnet_id = "${var.aws_subnet_id}"

  sw_version = "${var.sw_version}"
  aws_core_instance_count = "${var.aws_core_instance_count}"
  aws_instance_type = "${var.aws_instance_type}"
  aws_emr_version = "${var.aws_emr_version}"
  jupyter_name = "${var.jupyter_name}"

  emr_managed_master_security_group_id = "${module.emr_security.emr_managed_master_security_group_id}"
  emr_managed_slave_security_group_id = "${module.emr_security.emr_managed_slave_security_group_id}"
  emr_ec2_instance_profile_arn = "${module.emr_security.emr_ec2_instance_profile_arn}"
  emr_role_arn = "${module.emr_security.emr_role_arn}"
}
