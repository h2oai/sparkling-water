##
## Input Variables
##
variable "aws_access_key" {}
variable "aws_secret_key" {}

variable "aws_region" {
  default = "us-west-2"
}
variable "aws_availability_zone" {
  default = "us-west-2b"
}

variable "aws_subnet_id" {
  default = ""
}

variable "aws_vpc_id" {
  default = ""
}

variable "aws_ssh_public_key" {
  // This key is taken from https://0xdata.atlassian.net/wiki/spaces/SW/pages/1835106305/Testing+Infrastructure+on+AWS
  default = "AAAAB3NzaC1yc2EAAAADAQABAAACAQC9jhU/puhzV5yGxYUUXaIgC3mCJAaPGggYrc20vswt4Y+b73V35oTABFIeUxYBH/DOtbusXn/seqznmjNebIt87eqJR1qsQkoTpt+r52asPyBxan0H+V+L1bWG0GPBLP8zpCKrMT1w5uIa4NVlKX8iXyobZC8rdsJ3XDhNYkwLdVkAKkze1vWwiYzCUmfCwC7xxs0Hecld6msdFb4/0tgM41FBRZalPiQ2qlhKd8JMpQljcPHgMkm44FKS6aPIF0YzLRg28HXjXZMhYN+cutgk+KEj2L6GHMjyih/rsXIXTVVSVcqTRomXLvsE2TJ0MNvk1UFg6+4RzDbxR7VDhB67hOxFko2bidXJAwSB3hQmVzmZS22GC5cgFYaMC249QZtExvk5sHvXcnKqN4xNAGia2yWsbB77hvtLVDHK+H2/YPaj2K+XOKuEhNqdwOyRBCEyYk6Rimlzh88x61VpJ790/08xCZl2cr2PB3vIVO73MNEu0GzoeJxJf541sf+RwHMZw3yK/FFoGjzR0APCwxGQv4UM8qOZlF2bw0yVIH2djx7DeEGZqUhqsjzXN8eNi9L9QGtfHTiylQFrPi70+0LZj4GIa8dS2U9kvh0h77Nep3pYJFYuY1iGw7/naEbNexSRoo9JZyWeiOlwfr+l4K0xf7S/rjtHygbsjr74WJkDUw=="
}

variable "signing_file" {
  default = ""
}

variable "public_hostname" {
  default = ""
}

variable "github_key_file" {
  default = ""
}

variable "aws_key_file" {
  default = ""
}
