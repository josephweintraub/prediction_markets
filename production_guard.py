"""Require the documented EC2 checkout and data mount for production runs.

Imports, CLI help, and small fixture tests remain usable on the local checkout.
Production entrypoints invoke this guard before opening data or loading models.
"""

import os
from pathlib import Path
import platform


def require_production_host() -> None:
    """Reject production execution outside the canonical mounted environment."""
    if (
        platform.system() != "Linux"
        or Path(__file__).resolve().parent != Path("/home/ubuntu/prediction_markets")
        or not os.path.ismount("/mnt/data")
    ):
        raise RuntimeError(
            "Production computation is blocked outside the canonical EC2 environment. "
            "Run in /home/ubuntu/prediction_markets on Linux with /mnt/data mounted, "
            "using /home/ubuntu/venv/bin/python; see docs/EC2_SETUP.md."
        )
