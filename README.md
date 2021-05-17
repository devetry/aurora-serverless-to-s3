## RDS Aurora Serverless Snapshot Export to S3 Pipeline

This repository creates the automation necessary to export Amazon RDS Aurora _Serverless_ snapshots to S3 for a specific database whenever an automated snapshot is created. At present (2021-05-16), this is not a straightforward task for serverless RDS clusters. In fact, AWS support said it couldn't be done.

Aurora Serverless currently cannot dump to S3 from a snapshot. In order to work around this, we've come
up with four steps:

1. Listen for the creation of automated snapshots on the database we care about. Using the snapshot, restore a serverless snapshot to a _provisioned_ db.
2. Once the cluster is created, kick off a snapshot of that db.
3. When the snapshot completes, Export the snapshot of the provisioned db to S3.
4. When the export completes or fails, clean up the db and manual snapshot we created.

## Usage

1. Install the [Amazon Cloud Development Kit](https://aws.amazon.com/cdk/) (CDK).
2. Clone this repository and `cd` into it.
3. Modify the arguments to the `RdsSnapshotExportPipelineStack` constructor in `$/bin/cdk.ts` according to your environment.
    * `dbName`: This RDS database must already exist.
    * `s3BucketName`: An S3 bucket with the provided name will be created automatically for you.
4. Execute the following:
    * `npm install`
    * `npm run cdk bootstrap`
    * `npm run cdk deploy`
5. Open up your `<dbName>-rds-snapshot-exporter` function in the [AWS Lambda](https://console.aws.amazon.com/lambda/home) console and configure a test event using the contents of one of the [test events](./test-events) as a template.
    * **NOTE:** The example content is a *subset* of an SNS event notification containing the minimum valid event data necessary to successfully trigger the Lambda function's execution. You should modify the `<SNAPSHOT_NAME>` value within the `Message` key to match an existing RDS snapshot (e.g. `rds:<dbName>-YYYY-MM-DD-hh-mm`). You may also need to modify the `MessageId` if you are attempting to export the same snapshot more than once.
6. Click the **Test** button to start an export.

You can check on the progress of the export in the [Exports in Amazon S3](https://console.aws.amazon.com/rds/home#snapshots-list:tab=exporttos3) listing. When that is finished, you can use the [AWS Glue Crawler](https://console.aws.amazon.com/glue/home#catalog:tab=crawlers) that was created for you to crawl the export, then use [Amazon Athena](https://console.aws.amazon.com/athena/home#query) to perform queries on the exported snapshot.

## Cleanup

Execute `npm run cdk destroy` to delete resources pertaining to this example.

You will also need to delete the following manually:
   * The S3 bucket that was created to store the snapshot exports.
   * The [CDKToolkit CloudFormation Stack](https://console.aws.amazon.com/cloudformation/home#/stacks?filteringText=CDKToolkit) created by `npm run cdk bootstrap`.
   * The `cdktoolkit-stagingbucket-<...>` bucket.

## Demo

[![Demo](.github/demo-video.png)](https://www.youtube.com/watch?v=lyNGeDg6EII)

## License

This library is licensed under the MIT-0 License. See the [LICENSE](./LICENSE) file.
