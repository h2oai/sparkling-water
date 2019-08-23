#!/usr/bin/env bash

benchmark_execution_timeout=10800 # 3 hours

if [ -z "$aws_access_key" ]; then
    read -p "Enter your aws access key: "  aws_access_key
fi
if [ -z "$aws_secret_key" ]; then
    read -p "Enter your aws secret key: "  aws_secret_key
fi
if [ -z "$aws_ssh_public_key" ]; then
    read -p "Enter your aws ssh public key: "  aws_ssh_public_key
fi

cd "$(dirname "$0")"
script_path=$(pwd)
cd build/terraform/aws
terraform init

echo "Creating new cluster..."
output_block=$(terraform apply \
    -var "aws_access_key=$aws_access_key" \
    -var "aws_secret_key=$aws_secret_key" \
    -var "aws_ssh_public_key=$aws_ssh_public_key" \
    -auto-approve \
    | tee /dev/tty | tail -n 6)

# If cluster is established wait for benchmarks and download them
outputs_header=$(echo "$output_block" | head -n 1)
if [ "$outputs_header" == "Outputs:" ]; then
    echo "The new cluster created."

    results_url=$(echo "$output_block" | grep benchmark_results | cut -d ' ' -f 3)
    finished_file_url=$(echo "$output_block" | grep benchmark_finished_file | cut -d ' ' -f 3)

    # Wait until benchmarks are finished
    polling_step=5
    current_timeout="$benchmark_execution_timeout"
    printf 'Executing benchmarks...'
    until $(curl --output /dev/null --silent --head --fail "$finished_file_url"); do
        printf '.'
        sleep "$polling_step"
        current_timeout=$(expr $current_timeout - $polling_step)
        if [ "$current_timeout" -le 0 ]; then
            echo "" > /dev/stdout
            echo "Timeout $benchmark_execution_timeout seconds for finishing execution of benchmarks has expired." > /dev/stderr
            break
        fi
    done

    cd "$script_path"
    mkdir output
    curl "$results_url" --output output/results.tar.gz
    tar -zxvf output/results.tar.gz -C output
    rm output/results.tar.gz
else
    echo "Ignoring execution of benchmarks!" > /dev/stderr
fi

echo "Destroying the cluster ..."
cd build/terraform/aws
terraform destroy \
    -var "aws_access_key=$aws_access_key" \
    -var "aws_secret_key=$aws_secret_key" \
    -var "aws_ssh_public_key=$aws_ssh_public_key" \
    -auto-approve
echo "The cluster destroyed."
