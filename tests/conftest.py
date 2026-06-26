from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest


@pytest.fixture
def tmp_path(request: pytest.FixtureRequest) -> Path:
    safe_name = request.node.name.replace("[", "_").replace("]", "_").replace(" ", "_")
    path = Path("data/test_outputs/pytest_tmp") / f"{safe_name}_{uuid4().hex[:8]}"
    path.mkdir(parents=True, exist_ok=False)
    return path
