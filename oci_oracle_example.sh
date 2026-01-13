#!/bin/bash
# Example script showing how to use OCI Oracle backend with your database configuration

# Set the environment variables as you provided
export TNS_ADMIN="/Users/khoatran/Downloads/Wallet_khoatestdb"
export DB_USER="ADMIN"
export DB_PASSWORD="112233445566Aa@"
export DB_CONNECT_STRING="khoatestdb_high"

# Run the SGLang router with OCI Oracle backend
# The configuration will automatically pick up your environment variables
echo "Starting SGLang Router with OCI Oracle backend..."
echo "Using database: $DB_CONNECT_STRING"
echo "Wallet path: $TNS_ADMIN"
echo "User: $DB_USER"

# Example command (adjust worker URLs as needed)
./sgl-model-gateway/target/debug/sgl-model-gateway \
    --history-backend oci_oracle \
    --worker-urls http://localhost:8000 \
    --host 0.0.0.0 \
    --port 30000
