import os
import glob
import tempfile
import shutil
from shutil import copytree, ignore_patterns
from google.cloud import storage


def collect_files(source_dir):
    """
    Copies files to a temp directory and filters unwanted files.
    """
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
    """
    Upload files to GCS bucket with prefix.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for file_path in files:
        # Create destination path
        relative_path = os.path.relpath(file_path, temp_dir)
        dest_path = os.path.join(prefix, relative_path).replace("\\", "/")

        blob = bucket.blob(dest_path)
        blob.upload_from_filename(file_path)

        print(f"✅ Uploaded → gs://{bucket_name}/{dest_path}")


def upload_to_composer(source_dir, bucket_name, prefix):
    """
    Collect and upload files to Composer bucket.
    """
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
        description="Upload DAGs and data to Cloud Composer bucket"
    )

    # REQUIRED bucket argument
    parser.add_argument(
        "bucket_name",
        help="Cloud Composer bucket name (example: us-central1-xxxx-bucket)",
    )

    # Optional directories
    parser.add_argument(
        "--dags_directory",
        help="Local directory containing DAG files",
    )

    parser.add_argument(
        "--data_directory",
        help="Local directory containing data/scripts",
    )

    args = parser.parse_args()

    if not args.dags_directory and not args.data_directory:
        print("❌ Provide at least one directory to upload.")
        print("Example:")
        print("  --dags_directory dags")
        print("  --data_directory data")
        exit(1)

    if args.dags_directory:
        print("\n📤 Uploading DAGs...")
        upload_to_composer(args.dags_directory, args.bucket_name, "dags")

    if args.data_directory:
        print("\n📤 Uploading Data Files...")
        upload_to_composer(args.data_directory, args.bucket_name, "data")

    print("\n🎉 Upload completed successfully!")
