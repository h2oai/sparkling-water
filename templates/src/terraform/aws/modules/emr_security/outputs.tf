##
## Output variables - used or created resources
##
output "emr_managed_master_security_group_id" {
  value = "${aws_security_group.slave.id}"
}
output "emr_managed_slave_security_group_id" {
  value = "${aws_security_group.master.id}"
}
output "emr_ec2_instance_profile_arn" {
  value = "${aws_iam_instance_profile.emr_ec2_instance_profile.arn}"
}
output "emr_role_arn" {
  value = "${aws_iam_role.emr_role.arn}"
}
