##
## Provider definition
##
provider "aws" {
  region = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  version = "2.58"
}

resource "aws_ecr_repository" "sparkling_water_registry" {
  name                 = "opsh2oai/sparkling_water_tests"
  image_tag_mutability = "IMMUTABLE"

  tags = {
    Name = "opsh2oai/sparkling_water_tests"
    Owner = "oss-dev@h2o.ai"
    Department = "Engineering"
    Environment = "QA"
    Project = "SparklingWater"
    Scheduling = "AlwaysOn"
  }

  image_scanning_configuration {
    scan_on_push = true
  }
}
