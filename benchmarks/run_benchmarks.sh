#!/usr/bin/env bash

if [ -z "$timeout" ]; then
    timeout=10800 # 3 hours
fi
if [ -z "$aws_access_key" ]; then
    read -p "Enter your aws access key: "  aws_access_key
fi
if [ -z "$aws_secret_key" ]; then
    read -p "Enter your aws secret key: "  aws_secret_key
fi
if [ -z "$aws_ssh_public_key" ]; then
    read -p "Enter your aws ssh public key: "  aws_ssh_public_key
fi
if [ -z "$aws_instance_type" ]; then
    aws_instance_type="m5.2xlarge"
fi
if [ -z "$aws_core_instance_count" ]; then
    aws_core_instance_count=2
fi
if [ -z "$aws_emr_timeout" ]; then
    aws_emr_timeout="4 hours"
fi
if [ -z "$datasets" ]; then
    datasets="datasets.json"
fi
if [ -z "$driver_memory_gb" ]; then
    driver_memory_gb="8"
fi
if [ -z "$executor_memory_gb" ]; then
    executor_memory_gb="8"
fi
if [ -z "$run_yarn_internal" ]; then
    run_yarn_internal="true"
fi
if [ -z "$run_yarn_external" ]; then
    run_yarn_external="true"
fi
if [ -z "$run_local_internal" ]; then
    run_local_internal="true"
fi

cd "$(dirname "$0")"
script_path=$(pwd)
cd build/terraform/aws
terraform init

mac=$(curl "http://169.254.169.254/latest/meta-data/network/interfaces/macs/")
vpc=$(curl "http://169.254.169.254/latest/meta-data/network/interfaces/macs/${mac}/vpc-id")
subnet=$(curl "http://169.254.169.254/latest/meta-data/network/interfaces/macs/${mac}/subnet-id")
echo "Creating new cluster..."
output_block=$(terraform apply \
    -var "aws_access_key=$aws_access_key" \
    -var "aws_secret_key=$aws_secret_key" \
    -var "aws_ssh_public_key=$aws_ssh_public_key" \
    -var "aws_instance_type=$aws_instance_type" \
    -var "aws_core_instance_count=$aws_core_instance_count" \
    -var "aws_emr_timeout=$aws_emr_timeout" \
    -var "benchmarks_dataset_specifications_file=$datasets" \
    -var "benchmarks_other_arguments=$other_arguments" \
    -var "benchmarks_driver_memory_gb=$driver_memory_gb" \
    -var "benchmarks_executor_memory_gb=$executor_memory_gb" \
    -var "benchmarks_run_yarn_internal=$run_yarn_internal" \
    -var "benchmarks_run_yarn_external=$run_yarn_external" \
    -var "benchmarks_run_local_internal=$run_local_internal" \
    -var "aws_vpc_id=$vpc" \
    -var "aws_subnet_id=$subnet" \
    -auto-approve \
    | tee /dev/stderr | tail -n 6)

# If cluster is established wait for benchmarks and download them
outputs_header=$(echo "$output_block" | head -n 1)
if [ "$outputs_header" == "Outputs:" ]; then
    echo "The new cluster created."

    results_url=$(echo "$output_block" | grep benchmark_results | cut -d ' ' -f 3)
    finished_file_url=$(echo "$output_block" | grep benchmark_finished_file | cut -d ' ' -f 3)

    # Wait until benchmarks are finished
    polling_step=5
    current_timeout="$timeout"
    printf 'Executing benchmarks...'
    until $(curl --output /dev/null --silent --head --fail "$finished_file_url"); do
        printf '.'
        sleep "$polling_step"
        current_timeout=$(expr $current_timeout - $polling_step)
        if [ "$current_timeout" -le 0 ]; then
            echo "" > /dev/stdout
            echo "Timeout $timeout seconds for finishing execution of benchmarks has expired." > /dev/stderr
            break
        fi
    done

    if [ "$current_timeout" -gt 0 ]; then
        cd "$script_path"
        mkdir output
        curl "$results_url" --output output/results.tar.gz
        tar -zxvf output/results.tar.gz -C output
        rm output/results.tar.gz
    fi
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

if [ ! -d "$script_path/output" ]; then
    echo "The execution of benchmarks on AWS EMR went wrong!" > /dev/stderr
    exit 1
fi
if [ -z "$(ls -A $script_path/output)" ]; then
    echo "The output directory is empty!" > /dev/stderr
    exit 1
fi
