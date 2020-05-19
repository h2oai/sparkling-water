module "eks" {
  source = "terraform-aws-modules/eks/aws"
  cluster_name = local.cluster_name
  subnets = module.vpc.private_subnets
  wait_for_cluster_cmd = "until curl -k -s $ENDPOINT/healthz >/dev/null; do sleep 4; done"
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

data "aws_eks_cluster" "cluster" {
  name = module.eks.cluster_id
}

data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_id
}
