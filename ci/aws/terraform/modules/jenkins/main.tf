##
## Provider definition
##
provider "aws" {
  region = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  version = "2.58"
}

resource "aws_instance" "jenkins" {
  ami = "ami-0d1cd67c26f5fca19"
  instance_type = "t2.medium"
  subnet_id = var.aws_subnet_id
  vpc_security_group_ids = [aws_security_group.jenkins_security_group.id]
  associate_public_ip_address = true
  key_name = aws_key_pair.key.key_name

  root_block_device {
    volume_size = "80"
    volume_type = "standard"
  }

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
mkdir -p /home/ubuntu/.init
/usr/local/bin/aws s3 cp ${format("s3://%s", aws_s3_bucket.init_files_bucket.bucket)} /home/ubuntu/.init/ --recursive
/usr/local/bin/aws s3 rb ${format("s3://%s", aws_s3_bucket.init_files_bucket.bucket)} --force
chmod +x /home/ubuntu/.init/init.sh
chmod +x /home/ubuntu/.init/init-ssl.sh
chmod +x /home/ubuntu/.init/install-plugins.sh
sudo /home/ubuntu/.init/init.sh
EOF

  depends_on = [
    aws_s3_bucket_object.install_plugins,
    aws_s3_bucket_object.signing_file,
    aws_s3_bucket_object.init_ssl,
    aws_s3_bucket_object.init,
    aws_s3_bucket_object.github_key_file,
    aws_s3_bucket_object.init_jenkins,
    aws_s3_bucket_object.aws_key_file
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
  name    = var.public_hostname
  type    = "A"
  ttl     = "300"
  records = [
    aws_instance.jenkins.public_ip]
}

resource "aws_s3_bucket" "init_files_bucket" {
  force_destroy = true
  acl = "private"
}

resource "aws_s3_bucket_object" "init_jenkins" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "init_jenkins.groovy"
  acl = "private"
  source = "./modules/jenkins/scripts/init_jenkins.groovy"
}

resource "aws_s3_bucket_object" "init_ssl" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "init-ssl.sh"
  acl = "private"
  source = "./modules/jenkins/scripts/init-ssl.sh"
}

resource "aws_s3_bucket_object" "install_plugins" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "install-plugins.sh"
  acl = "private"
  source = "./modules/jenkins/scripts/install-plugins.sh"
}

resource "aws_s3_bucket_object" "init" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "init.sh"
  acl = "private"
  source = "./modules/jenkins/scripts/init.sh"
}

resource "aws_s3_bucket_object" "signing_file" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "secring.gpg"
  acl = "private"
  source = var.signing_file
}

resource "aws_s3_bucket_object" "github_key_file" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "git_private_key.pem"
  acl = "private"
  source = var.github_key_file
}

resource "aws_s3_bucket_object" "aws_key_file" {
  bucket = aws_s3_bucket.init_files_bucket.id
  key = "aws_private_key.pem"
  acl = "private"
  source = var.aws_key_file
}
