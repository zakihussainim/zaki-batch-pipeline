<#
.SYNOPSIS
    Recreates all AWS infrastructure for zaki-batch-pipeline (Project 1).
.DESCRIPTION
    Idempotent: safe to run multiple times. Checks whether each resource
    already exists before creating it, so a partial or repeat run won't
    error out on "already exists".
#>

param(
    [string]$Region = "eu-west-2",
    [Parameter(Mandatory = $true)]
    [string]$AlertEmail
)

$ErrorActionPreference = "Stop"

$AccountId = (aws sts get-caller-identity --query Account --output text)
if ($LASTEXITCODE -ne 0) {
    Write-Error "Could not reach AWS — check your AWS CLI credentials are configured (aws configure)."
    exit 1
}

$RawBucket         = "zaki-batch-pipeline-raw"
$CuratedBucket     = "zaki-batch-pipeline-curated"
$ScriptsBucket     = "zaki-batch-pipeline-scripts"
$AthenaBucket      = "zaki-batch-pipeline-athena-results"
$DataBuckets       = @($RawBucket, $CuratedBucket, $ScriptsBucket)
$TopicName         = "zaki-batch-pipeline-alerts"
$GlueRoleName      = "zaki-batch-pipeline-glue-role"
$CrawlerRoleName   = "zaki-batch-pipeline-crawler-role"
$JobName           = "zaki-batch-pipeline-clean-drawings"
$DatabaseName      = "zaki_batch_pipeline"
$CrawlerName       = "zaki-batch-pipeline-drawings-crawler"

Write-Host "Using AWS account $AccountId in $Region" -ForegroundColor Cyan

#  1. S3 data buckets 
foreach ($bucket in $DataBuckets) {
    aws s3api head-bucket --bucket $bucket 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[skip] Bucket $bucket already exists" -ForegroundColor Yellow
        continue
    }
    Write-Host "[create] Bucket $bucket"
    aws s3api create-bucket --bucket $bucket --region $Region `
        --create-bucket-configuration LocationConstraint=$Region
    if ($LASTEXITCODE -ne 0) { Write-Error "Failed creating bucket $bucket"; exit 1 }
}

'{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' |
    Out-File -FilePath "$PSScriptRoot\enc.json" -Encoding ascii -NoNewline

foreach ($bucket in $DataBuckets) {
    Write-Host "[configure] Locking down $bucket"
    aws s3api put-public-access-block --bucket $bucket `
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    aws s3api put-bucket-versioning --bucket $bucket --versioning-configuration Status=Enabled
    aws s3api put-bucket-encryption --bucket $bucket --server-side-encryption-configuration "file://$PSScriptRoot/enc.json"
}

# 2. Athena results bucket (no versioning — disposable query output) 
aws s3api head-bucket --bucket $AthenaBucket 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "[skip] Bucket $AthenaBucket already exists" -ForegroundColor Yellow
} else {
    Write-Host "[create] Bucket $AthenaBucket"
    aws s3api create-bucket --bucket $AthenaBucket --region $Region `
        --create-bucket-configuration LocationConstraint=$Region
    aws s3api put-public-access-block --bucket $AthenaBucket `
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    aws s3api put-bucket-encryption --bucket $AthenaBucket --server-side-encryption-configuration "file://$PSScriptRoot/enc.json"
}
Remove-Item "$PSScriptRoot\enc.json"

# 3. SNS topic + email subscription 
$TopicArn = (aws sns list-topics --query "Topics[?ends_with(TopicArn, ':$TopicName')].TopicArn" --output text)
if ([string]::IsNullOrWhiteSpace($TopicArn)) {
    Write-Host "[create] SNS topic $TopicName"
    $TopicArn = (aws sns create-topic --name $TopicName --query TopicArn --output text)
    aws sns subscribe --topic-arn $TopicArn --protocol email --notification-endpoint $AlertEmail
    Write-Host "Check $AlertEmail and confirm the subscription email, or alerts won't arrive." -ForegroundColor Yellow
} else {
    Write-Host "[skip] SNS topic $TopicName already exists" -ForegroundColor Yellow
}

# 4. IAM role for the Glue ETL job 
aws iam get-role --role-name $GlueRoleName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "[skip] IAM role $GlueRoleName already exists" -ForegroundColor Yellow
} else {
    Write-Host "[create] IAM role $GlueRoleName"
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}' |
        Out-File -FilePath "$PSScriptRoot\glue-trust.json" -Encoding ascii -NoNewline
    aws iam create-role --role-name $GlueRoleName --assume-role-policy-document "file://$PSScriptRoot/glue-trust.json"
    Remove-Item "$PSScriptRoot\glue-trust.json"
}

# Permissions policy re-applied every run (put-role-policy overwrites, so
# this naturally stays in sync — includes the s3:DeleteObject fix needed
# for the job's overwrite-mode writes to succeed on a second run).
$GluePermissionsJson = @"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadRawAndScript",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": [
        "arn:aws:s3:::$RawBucket/*",
        "arn:aws:s3:::$ScriptsBucket/*"
      ]
    },
    {
      "Sid": "ReadWriteCurated",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::$CuratedBucket",
        "arn:aws:s3:::$CuratedBucket/*"
      ]
    },
    {
      "Sid": "PublishAlerts",
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": "$TopicArn"
    },
    {
      "Sid": "GlueLogging",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:$($Region):$($AccountId):log-group:/aws-glue/*"
    }
  ]
}
"@
$GluePermissionsJson | Out-File -FilePath "$PSScriptRoot\glue-permissions.json" -Encoding ascii -NoNewline
aws iam put-role-policy --role-name $GlueRoleName --policy-name glue-job-permissions --policy-document "file://$PSScriptRoot/glue-permissions.json"
Remove-Item "$PSScriptRoot\glue-permissions.json"

$GlueRoleArn = "arn:aws:iam::${AccountId}:role/$GlueRoleName"

# 5. Upload script and sample data 
Write-Host "[upload] Script and sample raw data"
aws s3 cp "$PSScriptRoot\..\src\clean_drawings_metadata.py" "s3://$ScriptsBucket/clean_drawings_metadata.py"
aws s3 cp "$PSScriptRoot\..\data\drawings_metadata_raw.csv" "s3://$RawBucket/drawings_metadata_raw.csv"

# 6. Glue job 
aws glue get-job --job-name $JobName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "[skip] Glue job $JobName already exists" -ForegroundColor Yellow
} else {
    Write-Host "[create] Glue job $JobName"
    '{"Name":"glueetl","ScriptLocation":"s3://' + $ScriptsBucket + '/clean_drawings_metadata.py","PythonVersion":"3"}' |
        Out-File -FilePath "$PSScriptRoot\glue-command.json" -Encoding ascii -NoNewline
    $ArgsJson = '{"--RAW_PATH":"s3://' + $RawBucket + '/drawings_metadata_raw.csv",' +
                '"--CURATED_PATH":"s3://' + $CuratedBucket + '/data/",' +
                '"--QUARANTINE_PATH":"s3://' + $CuratedBucket + '/quarantine/",' +
                '"--SNS_TOPIC_ARN":"' + $TopicArn + '"}'
    $ArgsJson | Out-File -FilePath "$PSScriptRoot\glue-args.json" -Encoding ascii -NoNewline
    aws glue create-job `
        --name $JobName --role $GlueRoleArn `
        --command "file://$PSScriptRoot/glue-command.json" `
        --default-arguments "file://$PSScriptRoot/glue-args.json" `
        --glue-version "4.0" --number-of-workers 2 --worker-type G.1X
    Remove-Item "$PSScriptRoot\glue-command.json", "$PSScriptRoot\glue-args.json"
}

# 7. Glue Catalog database 
aws glue get-database --name $DatabaseName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "[skip] Glue database $DatabaseName already exists" -ForegroundColor Yellow
} else {
    Write-Host "[create] Glue database $DatabaseName"
    ('{"Name":"' + $DatabaseName + '"}') | Out-File -FilePath "$PSScriptRoot\db.json" -Encoding ascii -NoNewline
    aws glue create-database --database-input "file://$PSScriptRoot/db.json"
    Remove-Item "$PSScriptRoot\db.json"
}

#  8. IAM role for the crawler 
aws iam get-role --role-name $CrawlerRoleName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "[skip] IAM role $CrawlerRoleName already exists" -ForegroundColor Yellow
} else {
    Write-Host "[create] IAM role $CrawlerRoleName"
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}' |
        Out-File -FilePath "$PSScriptRoot\crawler-trust.json" -Encoding ascii -NoNewline
    aws iam create-role --role-name $CrawlerRoleName --assume-role-policy-document "file://$PSScriptRoot/crawler-trust.json"
    Remove-Item "$PSScriptRoot\crawler-trust.json"
}

$CrawlerPermissionsJson = @"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadCuratedData",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::$CuratedBucket",
        "arn:aws:s3:::$CuratedBucket/*"
      ]
    },
    {
      "Sid": "ManageThisDatabaseOnly",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
        "glue:CreateTable", "glue:UpdateTable",
        "glue:GetPartition", "glue:GetPartitions",
        "glue:BatchCreatePartition", "glue:BatchGetPartition", "glue:BatchDeletePartition",
        "glue:UpdatePartition", "glue:CreatePartition", "glue:DeletePartition"
      ],
      "Resource": [
        "arn:aws:glue:$($Region):$($AccountId):catalog",
        "arn:aws:glue:$($Region):$($AccountId):database/$DatabaseName",
        "arn:aws:glue:$($Region):$($AccountId):table/$DatabaseName/*"
      ]
    },
    {
      "Sid": "CrawlerLogging",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:$($Region):$($AccountId):log-group:/aws-glue/crawlers*"
    }
  ]
}
"@
$CrawlerPermissionsJson | Out-File -FilePath "$PSScriptRoot\crawler-permissions.json" -Encoding ascii -NoNewline
aws iam put-role-policy --role-name $CrawlerRoleName --policy-name crawler-permissions --policy-document "file://$PSScriptRoot/crawler-permissions.json"
Remove-Item "$PSScriptRoot\crawler-permissions.json"

$CrawlerRoleArn = "arn:aws:iam::${AccountId}:role/$CrawlerRoleName"

#  9. Crawler 
aws glue get-crawler --name $CrawlerName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "[skip] Crawler $CrawlerName already exists" -ForegroundColor Yellow
} else {
    Write-Host "[create] Crawler $CrawlerName"
    ('{"S3Targets":[{"Path":"s3://' + $CuratedBucket + '/data/"}]}') |
        Out-File -FilePath "$PSScriptRoot\crawler-targets.json" -Encoding ascii -NoNewline
    aws glue create-crawler `
        --name $CrawlerName --role $CrawlerRoleArn `
        --database-name $DatabaseName `
        --targets "file://$PSScriptRoot/crawler-targets.json"
    Remove-Item "$PSScriptRoot\crawler-targets.json"
    Write-Host "Run 'aws glue start-crawler --name $CrawlerName' to populate the Catalog table." -ForegroundColor Yellow
}

# 10. Athena workgroup output location 
Write-Host "[configure] Pointing Athena workgroup 'primary' at $AthenaBucket"
('{"ResultConfigurationUpdates":{"OutputLocation":"s3://' + $AthenaBucket + '/"}}') |
    Out-File -FilePath "$PSScriptRoot\workgroup-config.json" -Encoding ascii -NoNewline
aws athena update-work-group --work-group primary --configuration-updates "file://$PSScriptRoot/workgroup-config.json"
Remove-Item "$PSScriptRoot\workgroup-config.json"

Write-Host ""
Write-Host "Done. Run the pipeline and populate the Catalog with:" -ForegroundColor Green
Write-Host "  aws glue start-job-run --job-name $JobName"
Write-Host "  aws glue start-crawler --name $CrawlerName"