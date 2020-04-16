##
## Provider definition
##
provider "aws" {
  region = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

resource "aws_instance" "jenkins" {
  ami = "ami-0d1cd67c26f5fca19"
  instance_type = "t2.micro"
  subnet_id = var.aws_subnet_id
  vpc_security_group_ids = [aws_security_group.jenkins_security_group.id]
  associate_public_ip_address = true
  key_name = aws_key_pair.key.key_name


  tags = {
    Name = "Sparkling Water Jenkins"
  }

  user_data = <<EOF
#!/bin/bash
apt-get install unzip
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install
export AWS_ACCESS_KEY_ID=${var.aws_access_key}
export AWS_SECRET_ACCESS_KEY=${var.aws_secret_key}
/usr/local/bin/aws s3 cp  ${format("s3://%s", aws_s3_bucket.init_files_bucket.bucket)} . --recursive
./init.sh
EOF

  depends_on = [
    aws_s3_bucket_object.init_credentials,
    aws_s3_bucket_object.init_security,
    aws_s3_bucket_object.install_plugins,
    aws_s3_bucket_object.signing_file,
    aws_s3_bucket_object.init
  ]
}

resource "aws_key_pair" "key" {
  public_key = "ssh-rsa ${var.aws_ssh_public_key}"
}

resource "aws_security_group" "jenkins_security_group" {
  description = "Security group for master node"
  vpc_id = var.aws_vpc_id
  revoke_rules_on_delete = true

  // SSH Access to Jenkins Machine
  ingress {
    cidr_blocks = [
      "0.0.0.0/0"
    ]

    from_port = 22
    to_port = 22
    protocol = "tcp"
  }

  // Web Access to Jenkins Machine
  ingress {
    cidr_blocks = [
      "0.0.0.0/0"
    ]

    from_port = 443
    to_port = 443
    protocol = "tcp"
  }

  // Web Access to Jenkins Machine
  ingress {
    cidr_blocks = [
      "0.0.0.0/0"
    ]

    from_port = 0
    to_port = 80
    protocol = "tcp"
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = [
      "0.0.0.0/0"]
  }
}

data "aws_route53_zone" "h2o" {
  name         = "h2o.ai."
  private_zone = false
}

resource "aws_route53_record" "sparkling_jenkins" {
  zone_id = data.aws_route53_zone.h2o.zone_id
  name    = "sparkling-jenkins2.h2o.ai"
  type    = "A"
  ttl     = "300"
  records = [
    aws_instance.jenkins.public_ip]
}

resource "aws_s3_bucket" "init_files_bucket" {
  force_destroy = true
  acl = "private"
}

resource "aws_s3_bucket_object" "init_credentials" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "init_credentials.groovy"
  acl = "private"
  source = file("./modules/jenkins/scripts/init-credentials.groovy")
}

resource "aws_s3_bucket_object" "init_security" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "init_security.groovy"
  acl = "private"
  source = file("./modules/jenkins/scripts/init-security.groovy")
}

resource "aws_s3_bucket_object" "install_plugins" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "install_plugins.sh"
  acl = "private"
  source = file("./modules/jenkins/scripts/install-plugins.sh")
}

resource "aws_s3_bucket_object" "init" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "init.sh"
  acl = "private"
  source = file("./modules/jenkins/scripts/init.sh")
}

resource "aws_s3_bucket_object" "signing_file" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "secring.gpg"
  acl = "private"
  source = var.signing_file
}
