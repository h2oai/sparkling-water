resource "aws_security_group" "master" {
    name        = "aws_security_group_master"
    description = "Security group for master node"
    vpc_id      = "${aws_vpc.main.id}"

    ingress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
}


 resource "aws_security_group" "slave" {
     name        = "aws_security_group_slave"
     description = "Security group for worker node"
     vpc_id      = "${aws_vpc.main.id}"

     ingress {
         from_port   = 0
         to_port     = 0
         protocol    = "-1"
         cidr_blocks = ["0.0.0.0/0"]
     }

     egress {
         from_port   = 0
         to_port     = 0
         protocol    = "-1"
         cidr_blocks = ["0.0.0.0/0"]
     }
 }

