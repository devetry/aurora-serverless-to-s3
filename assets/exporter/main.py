import json
import logging
import os
import re

import boto3

"""
Aurora Serverless currently cannot dump to S3 from a snapshot. In order to work around this, we've come
up with four steps:
    1. Restore a serverless snapshot to a provisioned db. initiated upon receiving
        "RDS-EVENT-0091: Automated DB Snapshot has been created."
    2. Kick off a snapshot of that db. initiated upon receiving
        "RDS-EVENT-0008: DB Instance restored from snapshot"
    3. Export the snapshot of the provisioned db to S3. initiated upon receiving
        "RDS-EVENT-0042: Manual DB Snapshot has been created"
    4. Clean up the db we created. intiated upon receiving
        "RDS-EVENT-0161: DB snapshot export task completed."
        "RDS-EVENT-0159: DB snapshot export task failed.:

The handler defined in this file will perform the correct step depending on the SNS event it receives.
"""

DB_AUTOMATED_SNAPSHOT_CREATED = 'RDS-EVENT-0091'
DB_INSTANCE_RESTORED_FROM_SNAPSHOT = 'RDS-EVENT-0008'
MANUAL_SNAPSHOT_CREATED = 'RDS-EVENT-0042'
DB_SNAPSHOT_EXPORT_COMPLETED = 'RDS-EVENT-0161'
DB_SNAPSHOT_EXPORT_FAILED = 'RDS-EVENT-0159'

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", logging.INFO))


def restore_to_provisioned(snapshot_arn):
    print('Restoring', snapshot_arn, 'to a new db')

def make_manual_snapshot_of_provisioned(message):
    print('making manual snapshot of new db', json.dumps(message))

def kick_off_s3_export(snapshot_arn):
    print('kicking off s3 export', snapshot_arn)

def clean_up_provisioned_db(snapshot_arn):
    print('cleaning up provisioned db', snapshot_arn)

def handler(event, context):
    if event["Records"][0]["EventSource"] != "aws:sns":
        logger.warning(
            "This function only supports invocations via SNS events, "
            "but was triggered by the following:\n"
            f"{json.dumps(event)}"
        )
        return

    logger.debug("EVENT INFO:")
    logger.debug(json.dumps(event))

    message = json.loads(event["Records"][0]["Sns"]["Message"])

    if message["EventID"] == DB_AUTOMATED_SNAPSHOT_CREATED:
        restore_to_provisioned(message['SourceArn'])
    elif message['EventID'] == DB_INSTANCE_RESTORED_FROM_SNAPSHOT:
        make_manual_snapshot_of_provisioned(message)
    elif message['EventID'] == MANUAL_SNAPSHOT_CREATED:
        kick_off_s3_export(message['SourceArn'])
    elif message['EventID'] in (DB_SNAPSHOT_EXPORT_COMPLETED, DB_SNAPSHOT_EXPORT_FAILED):
        clean_up_provisioned_db(message['SourceArn'])

    # if message["Event ID"].endswith(os.environ["RDS_EVENT_ID"]) and re.match(
    #     "^rds:" + os.environ["DB_NAME"] + "-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}$",
    #     message["Source ID"],
    # ):
    #     export_task_identifier = event["Records"][0]["Sns"]["MessageId"]
    #     account_id = boto3.client("sts").get_caller_identity()["Account"]
    #     response = boto3.client("rds").start_export_task(
    #         ExportTaskIdentifier=(
    #             (message["Source ID"][4:27] + '-').replace("--", "-") + event["Records"][0]["Sns"]["MessageId"]
    #         ),
    #         SourceArn=f"arn:aws:rds:{os.environ['AWS_REGION']}:{account_id}:snapshot:{message['Source ID']}",
    #         S3BucketName=os.environ["SNAPSHOT_BUCKET_NAME"],
    #         IamRoleArn=os.environ["SNAPSHOT_TASK_ROLE"],
    #         KmsKeyId=os.environ["SNAPSHOT_TASK_KEY"],
    #     )
    #     response["SnapshotTime"] = str(response["SnapshotTime"])

    #     logger.info("Snapshot export task started")
    #     logger.info(json.dumps(response))
    else:
        logger.info(f"Ignoring event notification for {message['Source ID']}")
        logger.info(
            f"Function is configured to accept {os.environ['RDS_EVENT_ID']} "
            f"notifications for {os.environ['DB_NAME']} only"
        )
