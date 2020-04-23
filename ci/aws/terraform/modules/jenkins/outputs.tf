##
## Output variables
##

output "jenkins_url" {
  value = "https://${aws_instance.jenkins.public_dns}"
}
