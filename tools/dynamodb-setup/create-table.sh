#!/bin/sh

# shellcheck disable=SC2046
cd $(dirname "$0") && pwd

# shellcheck disable=SC2039
if [[ $# == 0 ]]; then
  echo "Parameters are empty."
  exit 1
fi

while getopts e: OPT
do
    # shellcheck disable=SC2220
    case ${OPT} in
        "e") ENV_NAME="$OPTARG" ;;
    esac
done

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-x}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-x}
export AWS_PAGER=""

if [[ $ENV_NAME = "prod" ]]; then

aws dynamodb create-table \
  --cli-input-json file://./journal-table.json

aws dynamodb create-table \
  --cli-input-json file://./snapshot-table.json

aws dynamodb create-table \
  --cli-input-json file://./state-table.json

else

DYNAMODB_ENDPOINT=${DYNAMODB_ENDPOINT:-localhost:8000}
echo "ENDPOINT = $DYNAMODB_ENDPOINT"

aws dynamodb create-table \
  --endpoint-url "http://$DYNAMODB_ENDPOINT" \
  --cli-input-json file://./journal-table.json

aws dynamodb create-table \
  --endpoint-url "http://$DYNAMODB_ENDPOINT" \
  --cli-input-json file://./snapshot-table.json

aws dynamodb create-table \
  --endpoint-url "http://$DYNAMODB_ENDPOINT" \
  --cli-input-json file://./state-table.json

fi


