import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVICES_DIR = REPO_ROOT / "services"


def load_module(unique_name: str, service: str, filename: str):
    """Load a service's module by file path under a unique name.

    Every service has its own consumer.py / producer.py, so importing them
    normally would collide in sys.modules. Loading each by absolute path
    under a unique alias lets this test exercise all 4 services' real,
    unmodified business logic against one real Kafka broker in a single
    process.
    """
    path = SERVICES_DIR / service / filename
    spec = importlib.util.spec_from_file_location(unique_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[unique_name] = module
    spec.loader.exec_module(module)
    return module
