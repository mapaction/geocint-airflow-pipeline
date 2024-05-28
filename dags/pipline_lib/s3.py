def create_file():
    with open('test.txt', 'w') as f:
        f.write('hello world')


def upload_to_s3(s3_bucket: str) -> None:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    print("s3 bucket:", s3_bucket)
    hook = S3Hook('aws_conn')
    hook.load_file(
        filename="./test.txt",
        key="some_key/test.txt",
        bucket_name=s3_bucket,
        replace=True
    )

