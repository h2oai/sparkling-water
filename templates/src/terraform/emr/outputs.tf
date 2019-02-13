##
## Output variables - used or created resources
##
 
output "emr_ec2_role" {
    value = "${aws_iam_role.emr_ec2_role.arn}"
}

output "emr_role" {
    value = "${aws_iam_role.emr_role.arn}"
}

output "emr_ec2_instance_profile" {
    value = "${aws_iam_instance_profile.emr_ec2_instance_profile.arn}"
}
