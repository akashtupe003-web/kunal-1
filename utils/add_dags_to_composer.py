import os
import glob
import tempfile
import shutil
from shutil import copytree, ignore_patterns
from google.cloud import storage


def collect_files(source_dir):
    """Copy files to temp dir & filter unwanted files."""
    if not os.path.exists(source_dir):
        print(f"❌ Directory '{source_dir}' not found.")
        return "", []

    temp_dir = tempfile.mkdtemp()

    copytree(
        source_dir,
        temp_dir,
        ignore=ignore_patterns("__init__.py", "*_test.py"),
        dirs_exist_ok=True,
    )

    files = [
        f for f in glob.glob(os.path.join(temp_dir, "**"), recursive=True)
        if os.path.isfile(f)
    ]

    return temp_dir, files


def upload_files(files, temp_dir, bucket_name, prefix):
    """Upload files to GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for file_path in files:
        # safe relative path
        relative_path = os.path.relpath(file_path, temp_dir)
        dest_path = os.path.join(prefix, relative_path).replace("\\", "/")

        blob = bucket.blob(dest_path)
        blob.upload_from_filename(file_path)

        print(f"✅ Uploaded → gs://{bucket_name}/{dest_path}")


def upload_to_composer(source_dir, bucket_name, prefix):
    temp_dir, files = collect_files(source_dir)

    if not files:
        print(f"⚠️ No files found in '{source_dir}'")
        return

    try:
        upload_files(files, temp_dir, bucket_name, prefix)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Upload DAGs & data to Cloud Composer bucket"
    )

    parser.add_argument("--dags_directory", help="Local DAG folder")
    parser.add_argument("--data_directory", help="Local data/scripts folder")

    # keep optional but validate manually
    parser.add_argument("--dags_bucket", help="Composer bucket name")

    args = parser.parse_args()

    # ✅ Validate required inputs
    if not args.dags_directory and not args.data_directory:
        parser.error("Provide --dags_directory and/or --data_directory")

    if not args.dags_bucket:
        parser.error("Provide --dags_bucket (Composer bucket name)")

    if args.dags_directory:
        print("\n📤 Uploading DAGs...")
        upload_to_composer(args.dags_directory, args.dags_bucket, "dags")

    if args.data_directory:
        print("\n📤 Uploading Data Files...")
        upload_to_composer(args.data_directory, args.dags_bucket, "data")

    print("\n🎉 Upload completed successfully!")
