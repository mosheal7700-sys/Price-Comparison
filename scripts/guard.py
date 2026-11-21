"""Decide whether a SCHEDULED attempt should collect, or skip because a collection ran recently.

A collection counts when it is the 'build' job of an earlier run of this workflow that finished successfully,
or is still running. The age is taken from GitHub's own run data (never from a cached page), so two attempts
can not both collect. If GitHub's API does not answer, the published data/index.json is used; if that fails too,
the attempt collects. Manual runs and pushes never reach this script's skip rule.
"""
import datetime as dt
import json
import os
import sys
import urllib.request

API = os.environ.get("GH_API", "https://api.github.com")
REPO = os.environ.get("REPO", "")
RUN_ID = os.environ.get("RUN_ID", "")
TOKEN = os.environ.get("GH_TOKEN", "")
WORKFLOW = os.environ.get("WORKFLOW_FILE", "update.yml")
INDEX_URL = os.environ.get("INDEX_URL", "")
EVENT = os.environ.get("EVENT", "")
MIN_GAP = int(os.environ.get("MIN_GAP_MIN", "105"))
BUILD_JOB = os.environ.get("BUILD_JOB", "build")


def get(url, auth=True):
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json", "User-Agent": "price-guard"})
    if auth and TOKEN:
        req.add_header("Authorization", "Bearer " + TOKEN)
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.load(r)


def parse(ts):
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def age_from_api():
    """minutes since the start of the latest real collection (build job) of another run, or None"""
    now = dt.datetime.now(dt.timezone.utc)
    runs = get(f"{API}/repos/{REPO}/actions/workflows/{WORKFLOW}/runs?per_page=12")["workflow_runs"]
    for run in runs:                                    # newest first
        if str(run["id"]) == str(RUN_ID):
            continue
        if run["status"] == "queued" or run["status"] == "waiting":
            continue
        for job in get(f"{API}/repos/{REPO}/actions/runs/{run['id']}/jobs?per_page=20")["jobs"]:
            if job["name"] != BUILD_JOB or not job.get("started_at"):
                continue
            if job["status"] == "in_progress" or job.get("conclusion") == "success":
                return int((now - parse(job["started_at"])).total_seconds() // 60), f"run {run['id']} ({job['status']}/{job.get('conclusion')})"
    return None, "no earlier collection found"


def age_from_site():
    now = dt.datetime.now(dt.timezone.utc)
    ix = get(INDEX_URL + ("&" if "?" in INDEX_URL else "?") + "nocache=" + str(int(now.timestamp())), auth=False)
    return int((now - parse(ix["generated_at"])).total_seconds() // 60), "published data/index.json"


def decide():
    if EVENT != "schedule":
        print(f"run of type {EVENT or '?'}: no interval check")
        return True
    age, src = None, ""
    try:
        age, src = age_from_api()
    except Exception as exc:
        print(f"GitHub API check failed ({str(exc)[:100]}), trying the published data")
        try:
            age, src = age_from_site()
        except Exception as exc2:
            print(f"published data check failed too ({str(exc2)[:100]})")
    if age is not None and age < MIN_GAP:
        print(f"skip: the last collection started {age} minutes ago (< {MIN_GAP}), source: {src}")
        return False
    print(f"collect: the last collection started {age if age is not None else 'unknown'} minutes ago, source: {src}")
    return True


if __name__ == "__main__":
    go = decide()
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as f:
            f.write("run=" + ("true" if go else "false") + "\n")
    sys.exit(0)
