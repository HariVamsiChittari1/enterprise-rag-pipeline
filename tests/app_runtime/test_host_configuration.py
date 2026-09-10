from __future__ import annotations

import json
from pathlib import Path


def test_durable_activities_are_limited_to_one_per_worker() -> None:
    host_config = json.loads(
        (Path(__file__).parents[2] / "app" / "host.json").read_text(
            encoding="utf-8"
        )
    )

    assert host_config["extensions"]["durableTask"][
        "maxConcurrentActivityFunctions"
    ] == 1