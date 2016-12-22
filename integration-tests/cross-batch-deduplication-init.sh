#!/bin/sh

# Helper script to initialize environment for Cross-batch deduplication tests
# ./3-enrich/scala-hadoop-shred/src/test/scala/com.snowplowanalytics.snowplow.enrich.hadoop/jobs/good/CrossBatchDeduplicationSpec.scala

set -e

DYNAMODB_DIR=/tmp/dynamodb
ARCHIVE_NAME=dynamodb_local_latest.tar.gz

echo 'Creating directories'
mkdir $DYNAMODB_DIR

echo 'Downloading DynamoDB local'
wget -P $DYNAMODB_DIR http://dynamodb-local.s3-website-us-west-2.amazonaws.com/$ARCHIVE_NAME

echo 'Unpacking DynamoDB local'
tar xvpf $DYNAMODB_DIR/$ARCHIVE_NAME -C $DYNAMODB_DIR

java -Djava.library.path=$DYNAMODB_DIR/DynamoDBLocal_lib -jar $DYNAMODB_DIR/DynamoDBLocal.jar -sharedDb
