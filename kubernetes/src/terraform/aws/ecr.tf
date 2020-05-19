
resource "aws_ecr_repository" "sw_kubernetes_repo" {
  name                 = "sw_kubernetes_repo/sparkling-water"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}
