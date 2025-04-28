import os
import time
import subprocess
from datetime import datetime, timezone
from flask import Flask, render_template, request, flash

app = Flask(__name__)
app.secret_key = os.urandom(24)

# ─── Configuration ────────────────────────────────────────────────────────────
PROJECT_ID     = "cipp-516-ecc"
CLUSTER_NAME   = "new-cluster2"
REGION         = "us-central1"
PYSPARK_SCRIPT = "gs://input-bucket-ecc/scripts/image_processing_v2.py"

# Predefined datasets mapping label to GCS prefix
# Dropdown datasets
DATASETS = {
    'Small':   'gs://input-bucket-ecc/caltech101/small/',
    'Medium': 'gs://input-bucket-ecc/caltech101/medium/',
    'Large':  'gs://input-bucket-ecc/caltech101/large/'
}

SPARK_UI_URL = "https://yycdgp44eveqfnyucsovqssriq-dot-us-central1.dataproc.googleusercontent.com/sparkhistory/"
# ────────────────────────────────────────────────────────────────────────────────

def submit_and_wait(gcloud_flags=None, script_args=None):
    """
    Submits a Dataproc Spark job, waits for it to finish, and returns
    (elapsed_seconds, logs) or (None, error_logs).
    """
    gcloud_flags = gcloud_flags or []
    script_args = script_args or []

    # Build the command:
    # 1) gcloud flags
    # 2) -- to separate script args
    submit_cmd = [
        "gcloud.cmd", "dataproc", "jobs", "submit", "pyspark", PYSPARK_SCRIPT,
        f"--cluster={CLUSTER_NAME}",
        f"--region={REGION}",
        f"--project={PROJECT_ID}",
        "--format=value(reference.jobId)"
    ] + gcloud_flags + [
        "--"
    ] + script_args

    # 1) Submit, capture jobId
    sub = subprocess.run(submit_cmd, capture_output=True, text=True)
    if sub.returncode != 0:
        return None, sub.stderr
    job_id = sub.stdout.strip()

    # 2) Wait for the job to finish
    start = time.time()
    wait_cmd = [
        "gcloud.cmd", "dataproc", "jobs", "wait", job_id,
        f"--region={REGION}",
        f"--project={PROJECT_ID}"
    ]
    wait = subprocess.run(wait_cmd, capture_output=True, text=True)
    elapsed = time.time() - start

    return elapsed, (wait.stdout or wait.stderr)


@app.route("/", methods=["GET", "POST"])
def index():
    metrics = None

    if request.method == "POST":
        # 1) Which dataset?
        label     = request.form["dataset"]
        in_prefix = DATASETS[label]

        # 2) Unique run_id: UTC timestamp
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        # 3) Parallel run (no extra gcloud flags)
        pp_flags = ["--properties", "spark.default.parallelism=8"]
        p_time, p_log = submit_and_wait(
            gcloud_flags=pp_flags,
            script_args=[in_prefix, run_id]
        )

        # 4) Sequential run (single partition)
        p_flags = ["--properties", "spark.default.parallelism=1"]
        s_time, s_log = submit_and_wait(
            gcloud_flags=p_flags,
            script_args=[in_prefix, run_id]
        )

        # 5) Prepare chart metrics if both succeeded
        if p_time is not None and s_time is not None:
            metrics = {
                "label":      label,
                "parallel":   round(p_time, 1),
                "sequential": round(s_time, 1)
            }

        # 6) Report any errors
        if p_time is None:
            flash(f"Parallel job error:\n{p_log}", "danger")
        if s_time is None:
            flash(f"Sequential job error:\n{s_log}", "danger")

    return render_template(
        "index.html",
        spark_ui_url=SPARK_UI_URL,
        datasets=DATASETS.keys(),
        metrics=metrics
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)