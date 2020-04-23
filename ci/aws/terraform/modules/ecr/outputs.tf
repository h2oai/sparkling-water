##
## Output variables
##

output "docker_registry_url" {
  value = aws_ecr_repository.sparkling_water_registry.repository_url
}

output "docker_registry_id" {
  value = aws_ecr_repository.sparkling_water_registry.registry_id
}
