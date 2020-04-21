module "network" {
  source = "./modules/network"
  aws_access_key = var.aws_access_key
  aws_secret_key = var.aws_secret_key
  aws_region = var.aws_region
  aws_availability_zone = var.aws_availability_zone
}

module "ecr" {
  source = "./modules/ecr"
  aws_access_key = var.aws_access_key
  aws_secret_key = var.aws_secret_key
  aws_region = var.aws_region
}

module "jenkins" {
  source = "./modules/jenkins"
  aws_access_key = var.aws_access_key
  aws_secret_key = var.aws_secret_key
  aws_region = var.aws_region
  aws_availability_zone = var.aws_availability_zone
  aws_subnet_id = module.network.aws_subnet_id
  aws_vpc_id = module.network.aws_vpc_id
  signing_file = var.signing_file
  public_hostname = var.public_hostname
  github_key_file = var.github_key_file
  aws_key_file = var.aws_key_file
}

module "ami" {
  source = "./modules/ami"
  aws_access_key = var.aws_access_key
  aws_secret_key = var.aws_secret_key
  aws_region = var.aws_region
}
