#!/bin/sh

cd $(dirname $0)

DYNAMODB_HOST=${DYNAMODB_HOST:-localhost}
DYNAMODB_PORT=${DYNAMODB_PORT:-8000}

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-x} 
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-x}
export AWS_PAGER=""

aws dynamodb create-table \
  --endpoint-url http://${DYNAMODB_HOST}:${DYNAMODB_PORT} \
  --cli-input-json file://./journal-table.json

aws dynamodb create-table \
  --endpoint-url http://${DYNAMODB_HOST}:${DYNAMODB_PORT} \
  --cli-input-json file://./snapshot-table.json
