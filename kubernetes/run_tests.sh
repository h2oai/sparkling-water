#!/usr/bin/env bash

if [ -z "$aws_access_key" ]; then
    read -p "Enter your aws access key: "  aws_access_key
fi
if [ -z "$aws_secret_key" ]; then
    read -p "Enter your aws secret key: "  aws_secret_key
fi

cd "$(dirname "$0")" || exit
cd src/terraform/aws || exit
terraform init

echo "Starting EKS"
terraform apply \
    -var "aws_access_key=$aws_access_key" \
    -var "aws_secret_key=$aws_secret_key" \
    -auto-approve


