import os
import subprocess
import json
from datetime import datetime
from flask import Flask, render_template, request, flash

app = Flask(__name__)
app.secret_key = os.urandom(24)

# ─── Configuration ────────────────────────────────────────────────────────────
PROJECT_ID     = "cipp-516-ecc"
CLUSTER_NAME   = "new-cluster2"
REGION         = "us-central1"
PYSPARK_SCRIPT = "gs://input-bucket-ecc/scripts/image_processing_v2.py"

DATASETS = {
    'Small':  'gs://input-bucket-ecc/caltech101/small/',
    'Medium': 'gs://input-bucket-ecc/caltech101/medium/',
    'Large':  'gs://input-bucket-ecc/caltech101/large/',
}

SPARK_UI_URL = (
    "https://yycdgp44eveqfnyucsovqssriq-"
    "dot-us-central1.dataproc.googleusercontent.com/sparkhistory/"
)
# ────────────────────────────────────────────────────────────────────────────────

def submit_and_wait(gcloud_flags=None, script_args=None):
    """
    1) Submit the PySpark job, get jobId
    2) Wait for it to finish
    3) Describe the job JSON to extract:
         - when it entered RUNNING (stateStartTime)
         - when it entered DONE   (status.stateStartTime)
    Returns (elapsed_seconds, logs) or (None, error_logs)
    """
    gcloud_flags = gcloud_flags or []
    script_args  = script_args  or []

    # 1) submit & capture jobId
    submit_cmd = [
        "gcloud.cmd", "dataproc", "jobs", "submit", "pyspark",
        PYSPARK_SCRIPT,
        f"--cluster={CLUSTER_NAME}",
        f"--region={REGION}",
        f"--project={PROJECT_ID}",
        "--format=value(reference.jobId)",
    ] + gcloud_flags + ["--"] + script_args

    sub = subprocess.run(submit_cmd, capture_output=True, text=True)
    if sub.returncode != 0:
        return None, sub.stderr.strip()
    job_id = sub.stdout.strip()

    # 2) wait for completion
    wait_cmd = [
        "gcloud.cmd", "dataproc", "jobs", "wait", job_id,
        f"--region={REGION}",
        f"--project={PROJECT_ID}",
    ]
    wait = subprocess.run(wait_cmd, capture_output=True, text=True)
    if wait.returncode != 0:
        return None, wait.stderr.strip()

    # 3) describe to get JSON
    desc_cmd = [
        "gcloud.cmd", "dataproc", "jobs", "describe", job_id,
        f"--region={REGION}",
        f"--project={PROJECT_ID}",
        "--format=json"
    ]
    desc = subprocess.run(desc_cmd, capture_output=True, text=True)
    if desc.returncode != 0:
        return None, desc.stderr.strip()

    info = json.loads(desc.stdout)
    status = info.get("status", {})
    history = info.get("statusHistory", [])

    # completion time = when status.stateStartTime for the final state (typically DONE)
    completion_ts = status.get("stateStartTime")

    # start time = when it entered RUNNING in history
    start_ts = None
    for rec in history:
        if rec.get("state") == "RUNNING":
            start_ts = rec.get("stateStartTime")
            break

    if not start_ts or not completion_ts:
        return None, "Could not parse start/completion timestamps"

    # parse and compute elapsed
    dt_start = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
    dt_end   = datetime.fromisoformat(completion_ts.replace("Z", "+00:00"))
    elapsed_seconds = (dt_end - dt_start).total_seconds()

    return elapsed_seconds, wait.stdout.strip()


@app.route("/", methods=["GET", "POST"])
def index():
    metrics = None

    if request.method == "POST":
        label     = request.form["dataset"]
        in_prefix = DATASETS[label]
        run_id    = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

        p_time, p_log = submit_and_wait(
            ["--properties", "spark.default.parallelism=8"],[in_prefix, run_id] )
        s_time, s_log = submit_and_wait(
            ["--properties", "spark.default.parallelism=1"],[in_prefix, run_id] )

        if p_time is not None and s_time is not None:
            metrics = {
                "label":      label,
                "parallel":   round(p_time / 60, 2),
                "sequential": round(s_time / 60, 2),
            }

        if p_time is None:
            flash(f"Parallel job error: {p_log}", "danger")
        if s_time is None:
            flash(f"Sequential job error: {s_log}", "danger")

    return render_template(
        "index.html",
        spark_ui_url=SPARK_UI_URL,
        datasets=DATASETS.keys(),
        metrics=metrics
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=True)
