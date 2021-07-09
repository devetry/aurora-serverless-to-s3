Sometimes a software task will seem simple, but it will prove to be stubbornly difficult. So difficult that you feel a need to justify the extraordinary amount of time and effort you're sinking into the problem. This is a story of a task like that.

![JIRA ticket titled "Set up a daily push of me3 data to ASU data lake"](https://clip.brianschiller.com/tPjKAlM-2021-07-08.png)

I would be working on the me3 project for a short time, alleviating some long-standing frustrations around CI and tackling a couple of troublesome tickets. In particular, this ticket to send a daily dump of our db to ASU's data lake.

Before I arrived, three other devs in had tried to knock this out. We'd been at it almost a year, on and off, without much success.

There's an AWS api call named [StartExportTask](https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_StartExportTask.html) that does exactly what we needed. You point it at a snapshot of a database and it writes the data to a designated S3 bucket. Easy Peasy. Theresa even found a neat CDK repo that wired up all the pieces we would need: [aws-samples/rds-snapshot-export-to-s3-pipeline](https://github.com/aws-samples/rds-snapshot-export-to-s3-pipeline). But it wasn't working
Only, our data is in an RDS Aurora _Serverless_ database, and the StartExportTask API call isn't supported for Serverless databases. Guess it's back to the drawing board.

Or is it?? How different is a snapshot of a serverless db from a snapshot of a provisioned db? Maybe we can turn one into the other?

I don't think you can inspect or mess with RDS snapshots, but you can definitely restore them to a database. In fact, we discovered you can restore an Aurora _Serverless_ snapshot to an Aurora _Serverfull_ cluster. Whoa. All of a sudden, StartExportTask is back on the table.

Theresa and I made the following plan:

1. Listen for the creation of automated snapshots on the database we care about. The aws-samples repo gave a good example of how to do this. Restore the serverless snapshot to a _provisioned_ db cluster.
2. Once the cluster is created, kick off a snapshot of that db.
3. When the snapshot completes, Export the snapshot of the provisioned db to S3.
4. When the export completes or fails, clean up the db and manual snapshot we created.

We ran through the steps once by hand to make sure everything was sound, and it worked! No more research, we could get to coding this thing!

Continuing to use the aws-samples repo as a model, we reworked it to listen to the events we needed, and added steps to for each of the four stages. Our lambda wouldn't be spinning its wheels the whole time, it would just activate in response to a notification of the cluster being created, or the snapshot completing, etc. After a bit of trial and error, we got it working smoothly. Our test bucket began to fill up with nightly snapshots of our db.

We reached out to the client and arranged for them to give our export-task role access to the bucket.

Because many projects would be writing to this bucket, the client wanted us to use an S3 access point. We couldn't find documentation one way or the other, but it seems like access points can't be used as the target of a StartExportTask.

We played around with permissions for a while, but ultimately decided to do another workaround: write the export to a bucket under our control and then copy the files over later. We could even set up the lambda to copy them at the same time we were cleaning up the resources we'd made during the process.

The last little bit of the problem was to copy those files from one bucket to another. I assumed `boto3` would have a method for this, and did some googling. Per Stack Overflow, that's not boto3's job, and we should be using the aws cli tool.

Fair enough. I wrote up a one-liner using the aws cli and updated the lambda to shell out to that instead. It failed with an error saying there was no program with that name. The lambda runtime I was in didn't include the aws cli.

Following some advice in a blog post from 2016 (experienced lambda users might already see where this is going), I wrote a script to package up the `aws` tool and its dependencies, including it in the lambda's assets: [package-aws.sh](https://github.com/devetry/aurora-serverless-to-s3/commit/beba8e712acc40dce7deb34f05d6f68e15acafb1#diff-0e515a1362a411b40b0c27584c0992f597c00d676c385e567b9e30580aa6b055). I suspect this might have worked fine, but I realized while I was working on it that this was what lambda layers are for.

Luckily, AWS CDK offers a pre-built layer for doing just that: [@awk-sdk/lambda-layer-aws-cli](https://github.com/aws/aws-cdk/tree/master/packages/%40aws-cdk/lambda-layer-awscli). I added it to my CDK stack and re-deployed.

This failed because my lambda was using the python 3.8 runtime, and the pre-built layer uses an old version of the AWS CLI that's only compatible with 2.7. But! It did offer a great example of how to put together a layer that's accessible from CDK.

I copied their file structure and went about making a layer and CDK package of my own. I got the docker image working with the new CLI version. But when I went to package and publish the app, I found that a bunch of the tools I needed were unpublished, and only available from within the `aws-cdk` package. So I cloned down that repo and copied my code over there.

That's the story so far. At this point I'm back to getting an AccessDenied error when I try to copy the files into the client's bucket. But it works when I copy them into another bucket that I own, so (_fingers crossed_) it's probably something on their end.
