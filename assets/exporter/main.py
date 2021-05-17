import json
import logging
import os
import re

import boto3

"""
Aurora Serverless currently cannot dump to S3 from a snapshot. In order to work around this, we've come
up with four steps:
    1. Restore a serverless snapshot to a provisioned db. initiated upon receiving
        "RDS-EVENT-0169: Automated DB Snapshot has been created."
    2. Kick off a snapshot of that db. initiated upon receiving
        "RDS-EVENT-0179: DB Instance restored from snapshot"
    3. Export the snapshot of the provisioned db to S3. initiated upon receiving
        "RDS-EVENT-0075: Manual DB Snapshot has been created"
    4. Clean up the db we created. initiated upon receiving
        "RDS-EVENT-0164: DB snapshot export task completed."
        "RDS-EVENT-0162: DB snapshot export task failed.:

The handler defined in this file will perform the correct step depending on the SNS event it receives.
"""

DB_AUTOMATED_SNAPSHOT_CREATED = 'http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.html#RDS-EVENT-0169'
DB_CLUSTER_CREATED = 'http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.html#RDS-EVENT-0170'
MANUAL_SNAPSHOT_CREATED = "http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.html#RDS-EVENT-0075"
DB_SNAPSHOT_EXPORT_COMPLETED = 'http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.html#RDS-EVENT-0164'
DB_SNAPSHOT_EXPORT_FAILED = 'http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.html#RDS-EVENT-0162'

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", logging.INFO))


def restore_to_provisioned(snapshot_arn):
    # eg "arn:aws:rds:us-west-2:100026411130:snapshot:rds:me3-dev-test-2021-05-12-10-40"
    snapshot_name = snapshot_arn.split(':')[-1]
    m = re.match(r'(.*)-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}', snapshot_name)
    if m is None:
        raise ValueError('could not find db name from snapshot name: ', snapshot_name)
    source_db_name = m.group(1)
    if source_db_name != os.environ['DB_NAME']:
        logger.info(f'ignoring snapshot for db {source_db_name}, as we only want to snapshot {os.environ["DB_NAME"]}')
        return
    dest_db_name = source_db_name + '-fordatalake'
    logger.info('Restoring ' + snapshot_arn + ' to a new db called ' + dest_db_name)
    rds = boto3.client('rds')
    result = rds.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=dest_db_name,
        Engine='aurora-postgresql',
        EngineMode='provisioned',
        EngineVersion='10.14',
        SnapshotIdentifier=snapshot_arn
    )
    # {'DBCluster': {
    #   'AllocatedStorage': 20, 'AvailabilityZones': ['us-west-2c', 'us-west-2b', 'us-west-2a'], 'BackupRetentionPeriod': 7,
    #   'DBClusterIdentifier': 'me3-dev-test-fordatalake', 'DBClusterParameterGroup': 'default.aurora-postgresql10', 'DBSubnetGroup': 'default',
    #   'Status': 'creating', 'Endpoint': 'me3-dev-test-fordatalake.cluster-cecef7typpch.us-west-2.rds.amazonaws.com', 'ReaderEndpoint': 'me3-dev-test-fordatalake.cluster-ro-cecef7typpch.us-west-2.rds.amazonaws.com', 'MultiAZ': False, 'Engine': 'aurora-postgresql', 'EngineVersion': '10.14', 'Port': 5432, 'MasterUsername': 'postgres', 'PreferredBackupWindow': '10:29-10:59', 'PreferredMaintenanceWindow': 'fri:09:59-fri:10:29', 'ReadReplicaIdentifiers': [], 'DBClusterMembers': [], 'VpcSecurityGroups': [{'VpcSecurityGroupId': 'sg-60460505', 'Status': 'active'}], 'HostedZoneId': 'Z1PVIF0B656C1W', 'StorageEncrypted': False, 'DbClusterResourceId': 'cluster-DTNIC67PVSKIDY4YD7TGGPUIC4',
    #   'DBClusterArn': 'arn:aws:rds:us-west-2:100026411130:cluster:me3-dev-test-fordatalake', 'AssociatedRoles': [], 'IAMDatabaseAuthenticationEnabled': False, 'ClusterCreateTime': datetime.datetime(2021, 5, 12, 18, 5, 29, 616000, tzinfo=tzlocal()), 'EngineMode': 'provisioned', 'DeletionProtection': False, 'HttpEndpointEnabled': False, 'CopyTagsToSnapshot': False, 'CrossAccountClone': False, 'DomainMemberships': [], 'TagList': []
    #  }, 'ResponseMetadata': {'RequestId': 'b102cd41-1318-4331-86b3-a578c456dc76', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': 'b102cd41-1318-4331-86b3-a578c456dc76', 'content-type': 'text/xml', 'content-length': '2602', 'vary': 'accept-encoding', 'date': 'Wed, 12 May 2021 18:05:29 GMT'}, 'RetryAttempts': 0}}
    rds.add_tags_to_resource(
        ResourceName=result['DBCluster']['DBClusterArn'],
        Tags=[{ 'Key': 'temporary:reason', 'Value': 'provisioned-for-backup-to-s3' }]
    )

def make_manual_snapshot_of_provisioned(db_arn):
    # db_arn is eg, "arn:aws:rds:us-west-2:100026411130:cluster:me3-dev-test-fordatalake"
    cluster_name = db_arn.split(':')[-1]
    if cluster_name != os.environ['DB_NAME'] + '-fordatalake':
        logger.info(f'ignoring snapshot for db {cluster_name}, as we only want to snapshot {os.environ["DB_NAME"]}')
        return
    logger.info('making manual snapshot of new db cluster ' + cluster_name)
    snapshot_name = os.environ['DB_NAME'] + '-snapshot'
    rds = boto3.client('rds')
    resp = rds.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier=snapshot_name,
        DBClusterIdentifier=cluster_name,
        Tags=[{ 'Key': 'temporary:reason', 'Value': 'provisioned-for-backup-to-s3' }]
    )

def kick_off_s3_export(event):
    # eg, "arn:aws:rds:us-west-2:100026411130:cluster-snapshot:me3-dev-test-snapshot"
    message = json.loads(event['Records'][0]['Sns']['Message'])
    snapshot_arn = message['Source ARN']
    snapshot_name = snapshot_arn.split(':')[-1]
    if snapshot_name != os.environ['DB_NAME'] + '-snapshot':
        logger.info(f'ignoring snapshot {snapshot_name}, as we only want to export for {os.environ["DB_NAME"]}')
        return
    logger.info('kicking off s3 export ' + snapshot_arn)
    response = boto3.client("rds").start_export_task(
        ExportTaskIdentifier=(
            message['Event Time'].split(' ')[0] + '-' + os.environ['DB_NAME'] + '-' + event["Records"][0]["Sns"]["MessageId"][:6]
        ),
        SourceArn=message['Source ARN'],
        S3BucketName=os.environ["SNAPSHOT_BUCKET_NAME"],
        IamRoleArn=os.environ["SNAPSHOT_TASK_ROLE"],
        KmsKeyId=os.environ["SNAPSHOT_TASK_KEY"],
    )
    response["SnapshotTime"] = str(response["SnapshotTime"])

    logger.info("Snapshot export task started")
    logger.info(json.dumps(response))

def clean_up_provisioned_db(snapshot_arn):
    snapshot_name = snapshot_arn.split(':')[-1]
    if snapshot_name != os.environ['DB_NAME'] + '-snapshot':
        logger.info(f'ignoring clean up request for {snapshot_name}, as we\'re only monitoring {os.environ["DB_NAME"]}')
        return
    logger.info('cleaning up provisioned db ' + snapshot_arn)
    rds = boto3.client('rds')
    rds.delete_db_cluster_snapshot(
        DBClusterSnapshotIdentifier=snapshot_name
    )
    rds.delete_db_cluster(
        DBClusterIdentifier=os.environ['DB_NAME'] + '-fordatalake',
        SkipFinalSnapshot=True
    )


def handler(event, context):
    if event["Records"][0]["EventSource"] != "aws:sns":
        logger.warning(
            "This function only supports invocations via SNS events, "
            "but was triggered by the following:\n"
            f"{json.dumps(event)}"
        )
        return

    logger.info("EVENT INFO:")
    logger.info(json.dumps(event))

    message = json.loads(event["Records"][0]["Sns"]["Message"])

    if message["Event ID"] == DB_AUTOMATED_SNAPSHOT_CREATED:
        # eg, {"Event Source":"db-snapshot","Event Time":"2021-05-12 10:41:00.185","Identifier Link":"https://console.aws.amazon.com/rds/home?region=us-west-2#snapshot:id=rds:me3-dev-test-2021-05-12-10-40","Source ID":"rds:me3-dev-test-2021-05-12-10-40","Source ARN":"arn:aws:rds:us-west-2:100026411130:snapshot:rds:me3-dev-test-2021-05-12-10-40","Event ID":"http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.html#RDS-EVENT-0090","Event Message":"Creating automated snapshot"}
        restore_to_provisioned(message['Source ARN'])
    elif message['Event ID'] == DB_CLUSTER_CREATED:
        # eg, {"Event Source":"db-cluster","Event Time":"2021-05-12 18:06:48.701","Identifier Link":"https://console.aws.amazon.com/rds/home?region=us-west-2#dbclusters:id=me3-dev-test-fordatalake","Source ID":"me3-dev-test-fordatalake","Source ARN":"arn:aws:rds:us-west-2:100026411130:cluster:me3-dev-test-fordatalake","Event ID":"http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.html#RDS-EVENT-0170","Event Message":"DB cluster created"}
        make_manual_snapshot_of_provisioned(message['Source ARN'])
    elif message['Event ID'] == MANUAL_SNAPSHOT_CREATED:
        # eg, {"Event Source":"db-cluster-snapshot","Event Time":"2021-05-13 17:49:53.624","Identifier Link":"https://console.aws.amazon.com/rds/home?region=us-west-2#snapshot:engine=aurora;id=me3-dev-test-fordatalake-snapshot","Source ID":"me3-dev-test-fordatalake-snapshot","Source ARN":"arn:aws:rds:us-west-2:100026411130:cluster-snapshot:me3-dev-test-fordatalake-snapshot","Event ID":"http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.html#RDS-EVENT-0075","Event Message":"Manual cluster snapshot created"}
        kick_off_s3_export(event)
    elif message['Event ID'] in (DB_SNAPSHOT_EXPORT_COMPLETED, DB_SNAPSHOT_EXPORT_FAILED):
        clean_up_provisioned_db(message['Source ARN'])

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

