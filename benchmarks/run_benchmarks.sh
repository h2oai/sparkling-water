#!/usr/bin/env bash

read -p "Enter your aws access key: "  aws_access_key
read -p "Enter your aws secret key: "  aws_secret_key
read -p "Enter your aws ssh public key: "  aws_ssh_public_key

cd "$(dirname "$0")"
cd build/terraform/aws
terraform init

output_block=$(terraform apply \
    -var "aws_access_key=$aws_access_key" \
    -var "aws_secret_key=$aws_secret_key" \
    -var "aws_ssh_public_key=$aws_ssh_public_key" \
    -auto-approve \
    | tee /dev/tty | tail -n 6)

# Kill job if there is no output section
outputs_header=$(echo "$output_block" | head -n 1)
if [ "$outputs_header" != "Outputs:" ]; then
    exit 1
fi

results_url=$(echo "$output_block" | grep benchmark_results | cut -d ' ' -f 3)
finished_file_url=$(echo "$output_block" | grep benchmark_finished_file | cut -d ' ' -f 3)


# Wait until benchmarks are finished
polling_step=5
timeout=3600
timeout_current="$timeout"
printf 'Executing benchmarks.'
until $(curl --output /dev/null --silent --head --fail "$finished_file_url"); do
    printf '.'
    sleep "$polling_step"
    timeout_current=$(expr $timeout_current - $polling_step)
    if [ "$timeout_current" -le 0 ]; then
        echo "Timeout $timeout seconds for finishing execution of benchmarks has expired." > /dev/stderr
        break
    fi
done
