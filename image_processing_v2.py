#!/usr/bin/env python3
import sys
import cv2
import numpy as np
from google.cloud import storage
from pyspark.sql import SparkSession

def list_images_in_prefix(gcs_prefix: str):
    """
    Given a GCS prefix like gs://bucket/caltech101/small/,
    return all .jpg/.png/.jpeg paths under it.
    """
    assert gcs_prefix.startswith("gs://"), "Prefix must start with gs://"
    bucket_name, blob_prefix = gcs_prefix[5:].split("/", 1)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=blob_prefix)
    return [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.lower().endswith((".jpg", ".jpeg", ".png"))
    ]

def process_image(gcs_path: str, run_id: str):
    """
    Download image, convert to grayscale, and upload to:
      gs://<output‑bucket>/processed/<run_id>/<original_filename>
    """
    # parse bucket + path
    bucket_name, blob_name = gcs_path[5:].split("/", 1)
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    data = blob.download_as_bytes()

    # decode & convert
    arr = np.frombuffer(data, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        return gcs_path, None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # re‑encode
    ok, buf = cv2.imencode(".jpg", gray)
    if not ok:
        return gcs_path, None

    # upload
    OUTPUT_BUCKET = "output-bucket-ecc"   # ← change to your bucket name
    out_blob_name = f"processed/{run_id}/{blob_name.split('/')[-1]}"
    out_blob = client.bucket(OUTPUT_BUCKET).blob(out_blob_name)
    out_blob.upload_from_string(buf.tobytes(), content_type="image/jpeg")
    return gcs_path, f"gs://{OUTPUT_BUCKET}/{out_blob_name}"

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: image_processing.py <gcs_prefix> <run_id>")
        sys.exit(1)

    gcs_prefix = sys.argv[1]     # e.g. gs://…/small/
    run_id     = sys.argv[2]     # timestamp or jobId

    spark = SparkSession.builder.appName("CloudImageProcessing").getOrCreate()
    sc = spark.sparkContext

    paths = list_images_in_prefix(gcs_prefix)
    if not paths:
        print(f"No images found under {gcs_prefix}")
        spark.stop()
        sys.exit(1)

    # parallel processing over the cluster (partitions default to #cores)
    results = (
        sc.parallelize(paths)
          .map(lambda p: process_image(p, run_id))
          .collect()
    )

    for orig, out in results:
        if out:
            print(f"✅ {orig} → {out}")
        else:
            print(f"❌ {orig}")

    spark.stop()