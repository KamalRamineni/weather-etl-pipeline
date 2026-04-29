"""
Lambda deployment script for the Weather ETL pipeline.

What it does:
1. Downloads Lambda-compatible (Linux x86_64) packages via pip
2. Copies source code into the package directory
3. Zips everything into weather_etl_lambda.zip
4. Creates an IAM execution role (if it doesn't exist)
5. Creates or updates the Lambda function
6. Creates an hourly EventBridge trigger

Usage:
    cd weather-etl-pipeline
    source ../venv/Scripts/activate
    python deploy/deploy.py
"""

import json
import os
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load .env from the project root (one level up from deploy/)
load_dotenv(Path(__file__).parent.parent / '.env')

# ── Config ────────────────────────────────────────────────────────────────────
REGION = os.getenv('AWS_REGION', 'us-east-2')
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
FUNCTION_NAME = 'weather-etl-pipeline'
IAM_ROLE_NAME = 'weather-etl-lambda-role'
EVENTBRIDGE_RULE = 'weather-etl-hourly'
RUNTIME = 'python3.12'
TIMEOUT = 300   # seconds (5 min — API + DB round-trip for 5 cities is ~10s, plenty of headroom)
MEMORY_MB = 512

PROJECT_ROOT = Path(__file__).parent.parent
DEPLOY_DIR = Path(__file__).parent
PACKAGE_DIR = DEPLOY_DIR / 'lambda_package'
ZIP_PATH = DEPLOY_DIR / 'weather_etl_lambda.zip'

# Source files/dirs to include in the Lambda zip
SOURCE_ITEMS = [
    'lambda_handler.py',
    'main.py',
    'config',
    'extractors',
    'transformers',
    'loaders',
    'sql',
]

# Environment variables to set on the Lambda function
LAMBDA_ENV = {k: v for k, v in {
    'OPENWEATHER_API_KEY': os.getenv('OPENWEATHER_API_KEY'),
    'DB_HOST':             os.getenv('DB_HOST'),
    'DB_PORT':             os.getenv('DB_PORT', '5432'),
    'DB_NAME':             os.getenv('DB_NAME', 'weather_db'),
    'DB_USER':             os.getenv('DB_USER', 'postgres'),
    'DB_PASSWORD':         os.getenv('DB_PASSWORD'),
    'S3_BUCKET_NAME':      S3_BUCKET,
    'CITIES':              os.getenv('CITIES', 'New York,London,Tokyo,Mumbai,Hyderabad'),
    'PYTHONIOENCODING':    'utf-8',
}.items() if v is not None}

# ── Steps ─────────────────────────────────────────────────────────────────────

def step(msg):
    print(f"\n{'='*55}\n{msg}\n{'='*55}")


def install_dependencies():
    step("Step 1: Install Linux-compatible dependencies")
    if PACKAGE_DIR.exists():
        shutil.rmtree(PACKAGE_DIR)
    PACKAGE_DIR.mkdir()

    # boto3 is pre-installed in Lambda — skip it to keep the zip small
    packages = [
        'pandas',
        'sqlalchemy',
        'psycopg2-binary',
        'requests',
        'python-dotenv',
    ]

    cmd = [
        sys.executable, '-m', 'pip', 'install',
        '--platform', 'manylinux2014_x86_64',
        '--target', str(PACKAGE_DIR),
        '--implementation', 'cp',
        '--python-version', '3.12',
        '--only-binary=:all:',
        '--upgrade',
        '--quiet',
        *packages,
    ]
    print(f"Running: pip install {' '.join(packages)}")
    subprocess.run(cmd, check=True)
    print(f"Installed to {PACKAGE_DIR}")


def copy_source():
    step("Step 2: Copy source code")
    for item in SOURCE_ITEMS:
        src = PROJECT_ROOT / item
        dst = PACKAGE_DIR / item
        if src.is_file():
            shutil.copy2(src, dst)
            print(f"  Copied file: {item}")
        elif src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
            print(f"  Copied dir:  {item}/")
        else:
            print(f"  Warning: {item} not found, skipping")


def build_zip():
    step("Step 3: Build zip archive")
    if ZIP_PATH.exists():
        ZIP_PATH.unlink()

    with zipfile.ZipFile(ZIP_PATH, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_path in sorted(PACKAGE_DIR.rglob('*')):
            if file_path.is_file():
                zf.write(file_path, file_path.relative_to(PACKAGE_DIR))

    size_mb = ZIP_PATH.stat().st_size / 1_048_576
    print(f"Created {ZIP_PATH.name} — {size_mb:.1f} MB (will deploy via S3)")


def get_or_create_role(iam):
    step("Step 4: IAM execution role")
    try:
        resp = iam.get_role(RoleName=IAM_ROLE_NAME)
        print(f"Role already exists: {IAM_ROLE_NAME}")
        return resp['Role']['Arn']
    except iam.exceptions.NoSuchEntityException:
        pass

    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    resp = iam.create_role(
        RoleName=IAM_ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(trust),
        Description='Execution role for weather ETL Lambda',
    )
    role_arn = resp['Role']['Arn']
    print(f"Created role: {IAM_ROLE_NAME}")

    iam.attach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
    )
    iam.put_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyName='weather-etl-s3',
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:GetObject"],
                "Resource": f"arn:aws:s3:::{S3_BUCKET}/*",
            }],
        }),
    )

    print("Waiting 12 s for IAM role to propagate...")
    time.sleep(12)
    return role_arn


def upload_zip_to_s3(s3_key='deploy/weather_etl_lambda.zip'):
    """Upload the zip to S3 (Lambda requires S3 for zips > 50 MB)."""
    print(f"Uploading zip to s3://{S3_BUCKET}/{s3_key}")
    s3 = boto3.client('s3', region_name=REGION)
    s3.upload_file(str(ZIP_PATH), S3_BUCKET, s3_key)
    print("Upload complete")
    return s3_key


def deploy_function(role_arn, lam, s3_key):
    step("Step 5: Deploy Lambda function")
    kwargs_config = dict(
        Timeout=TIMEOUT,
        MemorySize=MEMORY_MB,
        Environment={'Variables': LAMBDA_ENV},
    )
    code = {'S3Bucket': S3_BUCKET, 'S3Key': s3_key}
    try:
        lam.get_function(FunctionName=FUNCTION_NAME)
        print(f"Updating existing function: {FUNCTION_NAME}")
        lam.update_function_code(FunctionName=FUNCTION_NAME, **code)
        waiter = lam.get_waiter('function_updated')
        waiter.wait(FunctionName=FUNCTION_NAME)
        lam.update_function_configuration(FunctionName=FUNCTION_NAME, **kwargs_config)
    except lam.exceptions.ResourceNotFoundException:
        print(f"Creating new function: {FUNCTION_NAME}")
        lam.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime=RUNTIME,
            Role=role_arn,
            Handler='lambda_handler.handler',
            Code=code,
            **kwargs_config,
        )
    print(f"Lambda function ready: {FUNCTION_NAME}")


def create_schedule(lam, events):
    step("Step 6: EventBridge hourly trigger")
    resp = events.put_rule(
        Name=EVENTBRIDGE_RULE,
        ScheduleExpression='rate(1 hour)',
        State='ENABLED',
        Description='Trigger weather ETL pipeline every hour',
    )
    rule_arn = resp['RuleArn']

    func_arn = lam.get_function(FunctionName=FUNCTION_NAME)['Configuration']['FunctionArn']

    events.put_targets(
        Rule=EVENTBRIDGE_RULE,
        Targets=[{'Id': 'weather-etl-target', 'Arn': func_arn, 'Input': '{}'}],
    )

    try:
        lam.add_permission(
            FunctionName=FUNCTION_NAME,
            StatementId='eventbridge-hourly',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=rule_arn,
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceConflictException':
            pass  # permission already exists
        else:
            raise

    print(f"EventBridge rule active: {EVENTBRIDGE_RULE} (every 1 hour)")


def main():
    print("\n" + "=" * 55)
    print("  Weather ETL — Lambda Deployment")
    print("=" * 55)

    install_dependencies()
    copy_source()
    build_zip()

    iam = boto3.client('iam')
    lam = boto3.client('lambda', region_name=REGION)
    events = boto3.client('events', region_name=REGION)

    role_arn = get_or_create_role(iam)
    s3_key = upload_zip_to_s3()
    deploy_function(role_arn, lam, s3_key)
    try:
        create_schedule(lam, events)
    except ClientError as e:
        if 'AccessDenied' in e.response['Error']['Code']:
            print("\n  Note: EventBridge trigger needs extra IAM permissions.")
            print("  Set it up manually in the AWS Console (see README).")
        else:
            raise

    print("\n" + "=" * 55)
    print("  Deployment complete!")
    print(f"  Function : {FUNCTION_NAME}")
    print(f"  Region   : {REGION}")
    print(f"  Schedule : every 1 hour via EventBridge")
    print(f"  Logs     : CloudWatch > /aws/lambda/{FUNCTION_NAME}")
    print("=" * 55 + "\n")


if __name__ == '__main__':
    main()
