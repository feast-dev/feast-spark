# Trigger Github Actions PR Workflow by creating a workflow dispatch event via a POST request.
# Expects environment variables to be passed:
# PULL_PULL_SHA - pull request hea SHA.
# GCP_PROJECT_ID - GCP Project ID to pass to the PR workflow.
# GCP_SA_KEY - Service Account credentials in JSON format to pass to the PR workflow.
curl -X POST \
    -H "Accept: application/vnd.github.v3+json" \
    -d '{
        "ref": "${PULL_PULL_SHA}",
        "inputs": {
            "gcp_project_id": "${GCP_PROJECT_ID}"
            "gcp_sa_key": "${GCP_SA_KEY}"
        }
    }' \
    -u '' \
    'https://api.github.com/repos/mrzzy/feast-spark/actions/workflows/pr.yml/dispatches'
