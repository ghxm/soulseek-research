#!/bin/bash
# Deploy production infrastructure

set -e

echo "🚀 Deploying Soulseek Research Infrastructure..."

# Check if terraform.tfvars exists
if [ ! -f "terraform.tfvars" ]; then
    echo "❌ terraform.tfvars not found. Create it from terraform.tfvars.example first."
    exit 1
fi

# Initialize and apply Terraform
terraform init
terraform plan -out=tfplan
echo "Press Enter to proceed with deployment, Ctrl+C to cancel..."
read

terraform apply tfplan
rm -f tfplan

echo "✅ Infrastructure deployed successfully!"
echo "Database IP: $(terraform output -raw database_ip)"
terraform output client_ips