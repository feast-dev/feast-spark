from prometheus_client import Counter

job_whitelist_failure_count = Counter(
    "feast_job_whitelist_failure_count",
    "request failures due to feature table not being whitelisted",
    ["project", "table"],
)
job_submission_count = Counter(
    "feast_job_submission_count",
    "request to submit feast job",
    ["job_type", "project", "table"],
)
job_schedule_count = Counter(
    "feast_job_schedule_count", "request to schedule feast job", ["project", "table"]
)
