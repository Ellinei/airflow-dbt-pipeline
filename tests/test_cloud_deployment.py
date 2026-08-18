from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_requirements_pins_amazon_provider():
    requirements = (REPO_ROOT / "requirements.txt").read_text()
    assert "apache-airflow-providers-amazon==8.20.0" in requirements
