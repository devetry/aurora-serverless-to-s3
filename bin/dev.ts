#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { RdsSnapshotExportPipelineStack } from '../lib/rds-snapshot-export-pipeline-stack';

const app = new cdk.App();
new RdsSnapshotExportPipelineStack(app, 'RdsSnapshotExportToS3Pipeline', {
  dbName: 'me3-dev-db',
  s3BucketName: 'me3-serverless-aurora-export',
});
