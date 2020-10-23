resource "aws_security_group" "worker_group_mgmt_one" {
  name_prefix = "worker_group_mgmt_one"
  vpc_id = module.vpc.vpc_id
}

resource "aws_security_group_rule" "worker_group_mgmt_one_ingress" {
  type = "ingress"
  from_port = 0
  to_port = 0
  protocol = "-1"

  cidr_blocks = ["0.0.0.0/0"]
  security_group_id = "${aws_security_group.worker_group_mgmt_one.id}"
}

resource "aws_security_group_rule" "worker_group_mgmt_one_egress" {
  type = "egress"
  from_port = 0
  to_port = 0
  protocol = "-1"

  cidr_blocks = ["0.0.0.0/0"]
  security_group_id = "${aws_security_group.worker_group_mgmt_one.id}"
}
