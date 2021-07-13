import * as cdk from "@aws-cdk/core";
import * as path from "path";
import {CfnCrawler} from "@aws-cdk/aws-glue";
import {ManagedPolicy, PolicyDocument, Role, ServicePrincipal, AccountRootPrincipal} from "@aws-cdk/aws-iam";
import {Code, Function, Runtime} from "@aws-cdk/aws-lambda";
import {SnsEventSource} from "@aws-cdk/aws-lambda-event-sources";
import {Key} from "@aws-cdk/aws-kms";
import {CfnEventSubscription} from "@aws-cdk/aws-rds";
import {BlockPublicAccess, Bucket, CfnAccessPoint} from "@aws-cdk/aws-s3";
import { AwsCliLayer } from '@bgschiller/lambda-layer-awscli-2';
import {Topic} from "@aws-cdk/aws-sns";

export interface RdsSnapshotExportPipelineStackProps extends cdk.StackProps {
  /**
   * Name of the S3 bucket to which snapshot exports should be saved.
   *
   * NOTE: Bucket will be created if one does not already exist.
   */
  readonly s3BucketName: string;

  /**
   * Name of the database cluster whose snapshots the function supports exporting.
   */
  readonly dbName: string;
};

export class RdsSnapshotExportPipelineStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props: RdsSnapshotExportPipelineStackProps) {
    super(scope, id, props);

    // use an existing bucket
    const bucket = Bucket.fromBucketName(this, "SnapshotExportBucket", props.s3BucketName);
    // create a new bucket
    // const bucket = new Bucket(this, "SnapshotExportBucket", {
    //   bucketName: props.s3BucketName,
    //   blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
    // });

    const snapshotExportTaskRole = new Role(this, "SnapshotExportTaskRole", {
      assumedBy: new ServicePrincipal("export.rds.amazonaws.com"),
      description: "Role used by RDS to perform snapshot exports to S3",
      inlinePolicies: {
        "SnapshotExportTaskPolicy": PolicyDocument.fromJson({
          "Version": "2012-10-17",
          "Statement": [
            {
              "Action": [
                "s3:PutObject*",
                "s3:ListBucket",
                "s3:GetObject*",
                "s3:DeleteObject*",
                "s3:GetBucketLocation"
              ],
              "Resource": [
                `${bucket.bucketArn}`,
                `${bucket.bucketArn}/*`,
              ],
              "Effect": "Allow"
            }
          ],
        })
      }
    });

    const lambdaExecutionRole = new Role(this, "RdsSnapshotExporterLambdaExecutionRole", {
      assumedBy: new ServicePrincipal("lambda.amazonaws.com"),
      description: 'RdsSnapshotExportToS3 Lambda execution role for the "' + props.dbName + '" database.',
      inlinePolicies: {
        "SnapshotExporterLambdaPolicy": PolicyDocument.fromJson({
          "Version": "2012-10-17",
          "Statement": [
            {
              "Action": "rds:RestoreDBClusterFromSnapshot",
              "Resource": "*",
              "Effect": "Allow",
            },
            {
              "Action": "rds:StartExportTask",
              "Resource": "*",
              "Effect": "Allow",
            },
            {
              "Action": "rds:DescribeExportTasks",
              "Resource": "*",
              "Effect": "Allow",
            },
            {
              "Action": "rds:CreateDBClusterSnapshot",
              "Resource": "*",
              "Effect": "Allow"
            },
            {
              "Action": "rds:DeleteDBClusterSnapshot",
              "Resource": "*",
              "Effect": "Allow"
            },
            {
              "Action": "rds:DescribeDBClusterSnapshots",
              "Resource": "*",
              "Effect": "Allow"
            },
            {
              "Action": "rds:DeleteDBCluster",
              "Resource": "*",
              "Effect": "Allow"
            },
            {
              "Action": "rds:AddTagsToResource",
              "Resource": "*",
              "Effect": "Allow"
            },
            {
              "Action": "iam:PassRole",
              "Resource": [snapshotExportTaskRole.roleArn],
              "Effect": "Allow",
            },
            {
              "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:PutObject",
                "s3:PutObjectAcl"
              ],
              "Resource": [
                `${bucket.bucketArn}`,
                `${bucket.bucketArn}/*`,
                "arn:aws:s3:us-west-2:520882832350:accesspoint/eds-me3/object/raw/me3",
                "arn:aws:s3:us-west-2:520882832350:accesspoint/eds-me3/object/raw/me3/*",
                "arn:aws:s3:::asu-sdc-eds",
                "arn:aws:s3:::asu-sdc-eds/*",
                "arn:aws:s3:us-west-2:316793988975:accesspoint/eds-me3/object/raw/me3",
                "arn:aws:s3:us-west-2:316793988975:accesspoint/eds-me3/object/raw/me3/*",
              ],
              "Effect": "Allow"
          }
          ]
        })
      },
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
      ],
    });

    const snapshotExportGlueCrawlerRole = new Role(this, "SnapshotExportsGlueCrawlerRole", {
      assumedBy: new ServicePrincipal("glue.amazonaws.com"),
      description: "Role used by RDS to perform snapshot exports to S3",
      inlinePolicies: {
        "SnapshotExportsGlueCrawlerPolicy": PolicyDocument.fromJson({
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:GetObject",
                "s3:PutObject"
              ],
              "Resource": `${bucket.bucketArn}/*`,
            }
          ],
        }),
      },
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"),
      ],
    });

    const snapshotExportEncryptionKey = new Key(this, "SnapshotExportEncryptionKey", {
      alias: props.dbName + "-snapshot-exports",
      policy: PolicyDocument.fromJson({
        "Version": "2012-10-17",
        "Statement": [
          {
            "Principal": {
              "AWS": [
                (new AccountRootPrincipal()).arn,
                lambdaExecutionRole.roleArn,
                snapshotExportGlueCrawlerRole.roleArn
              ]
            },
            "Action": [
              "kms:Encrypt",
              "kms:Decrypt",
              "kms:ReEncrypt*",
              "kms:GenerateDataKey*",
              "kms:DescribeKey"
            ],
            "Resource": "*",
            "Effect": "Allow",
          },
          {
            "Principal": lambdaExecutionRole.roleArn,
            "Action": [
              "kms:CreateGrant",
              "kms:ListGrants",
              "kms:RevokeGrant"
            ],
            "Resource": "*",
            "Condition": {
                "Bool": {"kms:GrantIsForAWSResource": true}
            },
            "Effect": "Allow",
          }
        ]
      })
    });

    const snapshotEventTopic = new Topic(this, "AuroraServerlessSnapshotPipeline", {
      displayName: "aurora-serverless-snapshot-pipeline"
    });

    const sourceTypes = ['db-instance', 'db-cluster', 'db-snapshot', 'db-cluster-snapshot'];
    sourceTypes.forEach(sourceType =>  new CfnEventSubscription(this, 'RdsSnapshotEventNotification-' + sourceType, {
      snsTopicArn: snapshotEventTopic.topicArn,
      enabled: true,
      // eventCategories: ['creation', 'backup', 'restoration', 'notification'],
      sourceType,
    }));

    new Function(this, "LambdaFunction", {
      functionName: props.dbName + "-rds-snapshot-exporter",
      runtime: Runtime.PYTHON_3_8,
      handler: "main.handler",
      code: Code.fromAsset(path.join(__dirname, "/../assets/exporter/")),
      environment: {
        DB_NAME: props.dbName,
        LOG_LEVEL: "INFO",
        SNAPSHOT_BUCKET_NAME: bucket.bucketName,
        SNAPSHOT_TASK_ROLE: snapshotExportTaskRole.roleArn,
        SNAPSHOT_TASK_KEY: snapshotExportEncryptionKey.keyArn,
      },
      role: lambdaExecutionRole,
      timeout: cdk.Duration.seconds(30),
      events: [
        new SnsEventSource(snapshotEventTopic)
      ],
      layers: [
        new AwsCliLayer(this, 'AwsCliLayer'),
      ],
      memorySize: 256, // we max out our memory when we go to run `aws cp`.
    });

    new CfnCrawler(this, "SnapshotExportCrawler", {
      name: props.dbName + "-rds-snapshot-crawler",
      role: snapshotExportGlueCrawlerRole.roleArn,
      targets: {
        s3Targets: [
          {path: bucket.bucketName},
        ]
      },
      databaseName: props.dbName.replace(/[^a-zA-Z0-9_]/g, "_"),
      schemaChangePolicy: {
        deleteBehavior: 'DELETE_FROM_DATABASE'
      }
    });
  }
}
