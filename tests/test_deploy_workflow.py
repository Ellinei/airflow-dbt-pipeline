from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_deploy_workflow_is_manual_dispatch_only():
    workflow = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "deploy.yml").read_text())
    # YAML parses the bare `on:` key as the boolean True, not the string "on"
    triggers = workflow[True] if True in workflow else workflow["on"]
    assert set(triggers) == {"workflow_dispatch"}


def test_ci_workflow_is_untouched_by_deploy_triggers():
    ci = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text())
    triggers = ci[True] if True in ci else ci["on"]
    assert set(triggers) == {"push", "pull_request"}
