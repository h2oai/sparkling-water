##
## Provider definition
##
provider "aws" {
  region = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  version = "2.58"
}

resource "aws_ami" "jenkins-slave" {
  name = "Sparkling Water Jenkins Slave"
  root_device_name = "/dev/xvda"
  ena_support = true
  virtualization_type = "hvm"
  tags = {}
  timeouts {}
}
