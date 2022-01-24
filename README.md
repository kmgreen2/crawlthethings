# Crawl the Things

An extensible framework for crawling things on the web.

## Overview

Crawl the things was initially created to ingest and process news data from [Common Crawl](https://commoncrawl.org/), but can be
used to directly crawl and process pretty much anything.

There are three components to a crawler:

1. *Ingestor*: a component that implements the `Ingestor` interface.  An `Ingestor` implements a `next() -> Record` function
2. *Processor*: a component that implements the `Processor` interface.  A `Processor` implements a `process(record: Record)` function
3. *Storage*: provides a `StorageObject` abstraction.  A `StorageObject` exposes a `append(payload: string)` function

At a high-level, a crawl run will initialize an ingestor that emits records, which are processed by a processor whose
results are periodically flushed to a storage object.

Here is a simplified, single-threaded illustration (see `src/main.py` for the actual multi-threaded implementation):

```python
# Init in-memory results
results=[]
# Init my ingestor with ingestor specific params (e.g. WARC index for a WARC ingestor)
ingestor=MyIngestor('type', params)
# Init a storage object (e.g. local file or S3 object)
storage_object=StorageObject(params)
for record in ingestor:
    # Process a record generated by the ingestor and append the result to results
    result=MyProcessor.process(record)
    results.append(result)

    # Periodically flush the results to stable storage
    if len(results) > 0 and len(results) % 100 == 0:
        flush_results(storage_object, results)
        del results[:]
```

## Usage

```commandline
pipenv run python3 src/main.py -h
usage: main.py [-h] -i INPUT [-o OUTPUT] -p PROCESSOR -I INGESTOR [-t THREADS]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Input file containing ingest-specific configuration
  -o OUTPUT, --output OUTPUT
                        Output path (e.g. s3://<region>.<bucket>/<path> or file://<path>)
  -p PROCESSOR, --processor PROCESSOR
                        Processor to use (e.g. news)
  -I INGESTOR, --ingestor INGESTOR
                        Ingestor to use (e.g. warc-index)
  -t THREADS, --threads THREADS
                        Number of threads (default=16)
```

Note that the number of threads refers to the processor (e.g. news) threadpool.  The ingestor is single threaded.  To parallelize
the ingestion, you should partition the ingestion index and invoke `main.py` many times.

## Example Usage - Crawling the News

Wrapper scripts for crawling and processing news articles are provided in `projects/news`.

Here we will crawl a subset of news articles for March 2021.

1. Create virtual env and install packages: `pipenv install`
2. Set AWS credentials: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (authenticated access required for WARC as of 04/2022)
3. Run: `projects/news/ingest_month.sh 2021 03 0 1` (<year> <month> <offset> <length>)

When running the script you should see output like:

```commandline
2022-04-24 09:03:43,863 Fetching index entry: crawl-data/CC-NEWS/2021/03/CC-NEWS-20210301012354-00478.warc.gz

2022-04-24 09:03:43,864 Fetched index entry: crawl-data/CC-NEWS/2021/03/CC-NEWS-20210301012354-00478.warc.gz

2022-04-24 09:03:43,865 Downloading crawl-data/CC-NEWS/2021/03/CC-NEWS-20210301012354-00478.warc.gz to /tmp/23791229-921f-4877-b059-a652e9413c78
```

Downloading can take a while depending on your network card and location.  For example, with a home Internet connection
on the West coast, downloading each WARC takes over 10 minutes, while a `10G` interface in AWS takes seconds.

You will see tracebacks (usually from `lxml`, because not everything can be parsed).

As long as you see occasional `Appending 100 results` logs, the script is making progress.

Once the script completes, you should have a few artifacts:

1. An index file will get created at `./news-03-2021-0-1.files`, which contains the WARC index locations. In this case,
   there should only be one. Each month of news can be anywhere from 300-500 WARC index files. To process more,
   increase `length`.


   Since the indexes for a given month can consume 300-500 GB, in practice, you would likely invoke `ingest_month.sh` many times
   to process different WARC files in parallel; otherwise, you'll be waiting around a long time.

2. The results will be in `/tmp/crawlthenews-2021-03-0-1.json`. This is a big JSON array containing one article per
   entry. Each article will have the following data:

```json
{
  "uri": <source URL>
  "ts": <article timestamp>,
  "title": <article title>
  "text": <article body>
}
```

That's it!  Now you can download a bunch of news articles and run analysis (e.g. NLP) on the extracted text.

For scale, you'll likely need to run this in AWS of GCP.  For that, we use [Pulumi](#pulumi-and-docker) to create the cloud resources
for running a crawl.

## Ingestors

Currently, there are two `Ingestor` implementations: CSV and WARC ingestors. The CSV ingestor simply ingests a CSV file
and is mostly used for local testing. The WARC ingestor is initialized with a list of WARC index locations on S3. It
parses and iterates the indexes and emits the raw data for each WARC record.

For example, if you initialize the WARC ingestor with:
```commandline
crawl-data/CC-NEWS/2021/03/CC-NEWS-20210301012354-00478.warc.gz
crawl-data/CC-NEWS/2021/03/CC-NEWS-20210301040226-00479.warc.gz
crawl-data/CC-NEWS/2021/03/CC-NEWS-20210301060337-00480.warc.gz
crawl-data/CC-NEWS/2021/03/CC-NEWS-20210301074929-00481.warc.gz
crawl-data/CC-NEWS/2021/03/CC-NEWS-20210301091853-00482.warc.gz
```

It will sequentially process each index file by downloading it, iterating through each record and emitting the raw data (e.g. raw HTML)
for each record.

Note that each WARC index is roughly 1 GB, so you'll want to put processing close to the S3 bucket (e.g. us-east). In addition, if you
are processing a lot of WARC files, you should also spawn many crawl instances.

### Creating Other Ingestors

This framework can be used to ingest just about anything, provided you specify the locations to crawl:

- Web pages: create a `WebIngestor` that is initialized with a list of URLs and emits the raw HTML for each URL
- Files: create a `FileIngestor` that is initialized with a list of files and emits the contents of each file
- You get the idea...  It is pretty simple.

As of now, there is no reason to create an ingestor outside WARC, because, well, Common Crawl does the actual
crawling for us.  We just need to convert the indexes and records into a `Record` for processing.

To create a new ingestor, implement this interface, put the implementation in `src/ingestion` and add an initializer
for the ingestor in `src/main.py:main()`:

```python
class Ingestor(Iterable):
    def __next__(self) -> Record:
        return self.next()

    def __iter__(self):
        return self.iter()

    def next(self) -> Record:
        pass

    def iter(self):
        return self;
```

Note: ingestors are assumed to **not** be threadsafe.

## Processors

We currently have three processor implementations:

- *News*: a news processor built on a [fork of newspaper](https://github.com/kmgreen2/newspaper). Newspaper is a Python
  library that parses news websites. The maintainer has been inactive for years, so I created a fork to fix a few bugs,
  mostly around the text detection algorithm in newspaper.  This processor will detect and extract title, author and body
  from news articles, which can be used to perform analysis of historical news articles.
- *Copy*: a simple processor that simply copies the content from a `Record`.  This is mostly helpful in testing.
- *Rotten Tomatoes*: a processor that extracts audience and critic scores from Rotten Tomatoes pages.

### Creating Other Processors

To create a new processor, implement this interface, put the implementation in `src/processor` and add a call
to the processor in `src/main.py:do_process()`:

```python
class Processor:
    def _init__(self, results: List[Dict], mutex: threading.Lock):
        self.mutex = mutex
        self.results = results

    def process(self, record: Record):
        pass
```
Note: processors must serialize access to the shared results using the provided mutex.

## Storage

Two `StorageObject` implementations are provided:

- *S3*: storage object backed by an S3 object
- *file*: storage object backed by a local file

See the current implementation to add support to other types of backing stores.

## Pulumi and Docker

If you need to process a lot of data (e.g. many months or years of news), running the crawler locally will likely be too
slow.  We have provided the infra config needed to run multiple crawlers as AWS Batch jobs.  You will need both Docker
and Pulumi to deploy to AWS.

Be sure to [install Pulumi](https://www.pulumi.com/docs/get-started/aws/begin/)

Warning: Running in the cloud costs money.  Be sure to monitor your usage and potentially set limits on what you are willing
to spend.

### Create a Docker Image and Push to ECR

Before creating a new image, be sure to set your S3 bucket, if you want results to go to S3: `BUCKET_NAME=<your bucket>` in `projects/news/ingest_month.sh`


Your `ECR_ENDPOINT` should be something like `<id>.dkr.ecr.<region>.amazonaws.com`

From the root of this repo, generate a Docker image by running:
```
# aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin ${ECR_ENDPOINT}
# docker build . -f Dockerfile.processor -t ${ECR_ENDPOINT}/processor:latest
# docker push ${ECR_ENDPOINT}/processor:latest
```

### Initialize a New Pulumi Stack

First, change to the `infra/jobs` directory.

Next, run `pulumi stack init` (or `pulumi stack init  --secrets-provider <kms uri>` if you want to use a KMS).  This will
create a file called `Pulumi.<name-you-provided>.yaml`.  Open the file and edit as you see fit.  Of course, you'll have to
provide your own public key (optional) and VPC id:

```yaml
  secretsprovider: <this should be your KMS, if you chose to go that route>
  aws:region: us-west-2
  jobs:instance_type: c6g.4xlarge
  jobs:nat_subnet_cidr: 10.1.2.0/24
  jobs:public_key: <your SSH public key>
  jobs:ssm_arn: arn:aws:iam::884270592710:policy/SessionManagerPermissions
  jobs:subnet_cidr: 10.1.1.0/24
  jobs:vpc_id: <your VPC id>
```

Next, ensure you have a valid stack by previewing: `pulumi preview -s <name-you-provided>`:

```commandline
Previewing update (<name-you-provided>):
     Type                              Name                                     Plan
 +   pulumi:pulumi:Stack               jobs-test                                create
 +   ├─ aws:iam:Role                   awsBatchServiceRoleRole                  create
 +   ├─ aws:iam:Role                   ecsInstanceRoleRole                      create
 +   ├─ aws:ec2:Subnet                 natSubnet                                create
 +   ├─ aws:ec2:Subnet                 crawlTheThingsSubnet                     create
 +   ├─ aws:ec2:SecurityGroup          crawlTheThingsVpcSecurityGroup           create
 +   ├─ aws:ec2:Eip                    natEip                                   create
 +   ├─ aws:ec2:KeyPair                key_pair                                 create
 +   ├─ aws:ec2:InternetGateway        igw                                      create
 +   ├─ aws:iam:RolePolicyAttachment   awsBatchServiceRoleRolePolicyAttachment  create
 +   ├─ aws:iam:RolePolicyAttachment   ecsInstanceRoleRolePolicyAttachment      create
 +   ├─ aws:iam:RolePolicyAttachment   ssmRolePolicyAttachment                  create
 +   ├─ aws:iam:InstanceProfile        ecsInstanceRoleInstanceProfile           create
 +   ├─ aws:ec2:RouteTable             egressRoutes                             create
 +   ├─ aws:ec2:NatGateway             nat                                      create
 +   ├─ aws:ec2:RouteTable             natRoutes                                create
 +   ├─ aws:ec2:RouteTableAssociation  egressRouteAssociation                   create
 +   ├─ aws:batch:ComputeEnvironment   crawlTheThings                           create
 +   ├─ aws:ec2:RouteTableAssociation  natRouteAssociation                      create
 +   └─ aws:batch:JobQueue             crawlTheThings                           create

Resources:
    + 20 to create
```

If you do not get errors, run `pulumi up -s <name-you-provided>`.  Creation should take a few minutes.

Next, navigate to `https://us-west-2.console.aws.amazon.com/batch`.  You should see a Job Queue and Compute Environment.

Now, create a Job definition:

1. Under job definitions, hit `Create`
2. Choose an appropriate `Name` and set a decent timeout (maybe an hour)
3. Platform compatibility should be `EC2`
4. Job configuration:
   1. The `Image` should be `ECR_ENDPOINT/processor:latest` (you need to specify your ECR endpoint)
   2. The `Command` should be `/main/projects/news/ingest_month.sh 2021 06 0 5`
   3. I'd choose `16` vCPUs and `16GB` of memory (this could change depending on your instance type)

Finally, let's run the job by selecting the new `Job Definition` and clicking `Submit new job`:

1. Provide a `name`
2. Select the newly created job definition
3. Select the `Job queue` with the prefix `crawlTheThings`
4. Set the following environment variables in the `Job configuration` section: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
5. You will also need to set `AWS_S3_HOST` to `s3-<s3-bucket-region>.amazonaws.com`

You should be able to monitor progress in the AWS Batch Dashboard. Note, this will spin up resources in ECS and EC2
on-demand, so it may be a few minutes before the job starts.
