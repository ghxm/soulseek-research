#!/bin/bash
# Destroy production infrastructure

set -e

echo "🛑 Destroying Soulseek Research Infrastructure..."

if [ ! -f "terraform.tfstate" ]; then
    echo "❌ No terraform state found. Nothing to destroy."
    exit 0
fi

terraform plan -destroy
echo "Press Enter to proceed with destruction, Ctrl+C to cancel..."
read

terraform destroy -auto-approve

echo "✅ Infrastructure destroyed successfully!"