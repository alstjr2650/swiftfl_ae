"""Make the workspace's optional traffic dependencies importable."""

import os
import sys
from pathlib import Path


def use_workspace_dependencies():
    folder = str(Path(__file__).resolve().parents[1] / '.traffic-deps')
    if Path(folder).is_dir():
        sys.path.insert(0, folder)
        existing = os.environ.get('PYTHONPATH')
        os.environ['PYTHONPATH'] = os.pathsep.join(filter(None, (folder, existing)))
