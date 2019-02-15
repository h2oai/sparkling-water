 ##
 ## Output variables
 ##

 output "aws_vp_id" {
     value = "${aws_vpc.main.id}"
 }

 output "aws_subnet_id" {
     value = "${aws_subnet.main.id}"
 }

