import os
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud import bigquery


def test_gcs_bucket_access():
    """Check we can access the specific bucket (no bucket listing required)."""
    bucket_name = os.environ["GCS_BUCKET"]
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Calls GCS API to verify the bucket exists + we have permission to see it
    bucket.reload()

    print("\n✅ GCS bucket access successful")
    print(f"Bucket exists & accessible: gs://{bucket_name}")


def test_gcs_write():
    """Check we can write an object into the bucket (what the pipeline needs)."""
    bucket_name = os.environ["GCS_BUCKET"]
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob("tests/hello.txt")
    blob.upload_from_string(f"hello {datetime.now(timezone.utc).isoformat()}\n")

    print("\n✅ GCS write successful")
    print(f"Wrote: gs://{bucket_name}/tests/hello.txt")


def test_bigquery_query():
    """Check we can run a basic BigQuery job."""
    client = bigquery.Client()
    result = client.query("SELECT 1 AS test_column").result()

    for row in result:
        print("\n✅ BigQuery query successful")
        print(f"Query result: {row.test_column}")


if __name__ == "__main__":
    print("Testing Google Cloud connections...")

    # Helpful debug prints (safe):
    print(f"GCP project detected: {storage.Client().project}")
    print(f"GCS_BUCKET env var: {os.environ.get('GCS_BUCKET')}")

    test_gcs_bucket_access()
    test_gcs_write()
    test_bigquery_query()

    print("\n🎉 All cloud checks passed!")