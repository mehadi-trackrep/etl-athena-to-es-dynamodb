#!/bin/bash

# Exit on failure
set -e

echo "🔐 Loading environment variables from .env..."

# Safely load .env file (handles comments and empty lines)
set -o allexport
if [[ -f .env ]]; then
  source .env
else
  echo "❌ .env file not found!"
  exit 1
fi
set +o allexport

# Validate required variables
: "${AWS_ACCESS_KEY_ID:?Missing AWS_ACCESS_KEY_ID in .env}"
: "${AWS_SECRET_ACCESS_KEY:?Missing AWS_SECRET_ACCESS_KEY in .env}"
: "${AWS_REGION:?Missing AWS_REGION in .env}"
: "${AWS_ACCOUNT_ID:?Missing AWS_ACCOUNT_ID in .env}"

echo "✅ Environment variables loaded."

ECR_REPOSITORY=common-for-all
TAG=data_engineering_repo

ECR_URL="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "📦 Logging in to AWS ECR..."
aws ecr get-login-password --region "${AWS_REGION}" | \
docker login --username AWS --password-stdin 143728503219.dkr.ecr.eu-west-1.amazonaws.com

sudo docker build --platform linux/arm64 -t "${ECR_REPOSITORY}:${TAG}" .
sudo docker tag "${ECR_REPOSITORY}:$TAG" "${ECR_URL}/${ECR_REPOSITORY}:$TAG"
sudo docker push "${ECR_URL}/${ECR_REPOSITORY}:$TAG"