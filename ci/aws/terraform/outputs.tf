##
## Output variables
##

output "docker_registry_url" {
  value = module.ecr.docker_registry_url
}

output "docker_registry_id" {
  value = module.ecr.docker_registry_id
}

output "jenkins_url" {
  value = module.jenkins.jenkins_url
}
