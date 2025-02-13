#!/usr/bin/python
"""
Sends post request to trigger GitHub Action

"""
import os
import requests

GITHUB_TOKEN = os.getenv("GH_TOKEN")


def trigger_workflow(account_name, repo_name, action_name, action_inputs={}):
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
    }
    payload = {"ref": "main", "inputs": action_inputs}
    response = requests.post(
        f"https://api.github.com/repos/{account_name}/{repo_name}/actions/workflows/{action_name}/dispatches",
        headers=headers,
        json=payload,
    )

    if response.status_code == 204:
        print(
            f"GitHub Actions workflow triggered successfully with inputs: {action_inputs}"
        )
    else:
        raise Exception(f"Error triggering workflow: {response.content}")