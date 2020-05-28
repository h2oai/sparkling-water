module "eks" {
  source = "terraform-aws-modules/eks/aws"
  cluster_name = local.cluster_name
  subnets = module.vpc.private_subnets
  vpc_id = module.vpc.vpc_id

  worker_groups = [
    {
      name = "worker-group-1"
      instance_type = "t2.medium"
      additional_userdata = "echo foo bar"
      additional_security_group_ids = [
        aws_security_group.worker_group_mgmt_one.id]
      asg_max_size = 4
      asg_min_size = 4
      asg_desired_capacity = 4
    },
  ]
}

provider "kubernetes" {
  host = data.aws_eks_cluster.cluster.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.cluster.certificate_authority.0.data)
  token = data.aws_eks_cluster_auth.cluster.token
  load_config_file = false
  version = "~> 1.11"
}

data "aws_eks_cluster" "cluster" {
  name = module.eks.cluster_id
}

data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_id
}
