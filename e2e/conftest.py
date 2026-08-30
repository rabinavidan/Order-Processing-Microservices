import importlib
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVICES_DIR = REPO_ROOT / "services"

# Every service's modules use plain sibling imports (e.g. consumer.py does
# `from db import ...`), so importing two services' same-named modules in
# one process needs care: each service's import phase gets its own
# sys.path entry and a clean sys.modules slate for these names, so its
# sibling imports resolve against ITS OWN files, not another service's.
_SHARED_MODULE_NAMES = ["db", "models", "producer", "consumer", "dlq_producer", "outbox_relay", "main"]


def load_service_modules(service: str, *names: str) -> dict:
    """Import `names` from `services/<service>/`, returning {name: module}.

    Loads all 4 services' real, unmodified business logic in one process by
    scoping each service's import phase: put its directory on sys.path,
    clear cached modules that would otherwise collide (db, consumer, etc.),
    import what's needed, then restore state — the caller keeps direct
    references to the returned module objects even after they're evicted
    from sys.modules for the next service's phase.
    """
    service_dir = SERVICES_DIR / service
    sys.path.insert(0, str(service_dir))
    saved = {name: sys.modules.pop(name) for name in _SHARED_MODULE_NAMES if name in sys.modules}
    try:
        loaded = {name: importlib.import_module(name) for name in names}
    finally:
        sys.path.remove(str(service_dir))
        for name in _SHARED_MODULE_NAMES:
            sys.modules.pop(name, None)
        sys.modules.update(saved)
    return loaded
