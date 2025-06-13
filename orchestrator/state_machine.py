import os
import json

# Allowed transitions (updated with your correction)
TRANSITIONS = {
    "init_startup": ["in_startup_processing"],
    "in_startup_processing": ["active", "init_shutdown"],
    "active": ["init_shutdown"],
    "init_shutdown": ["in_shutdown_processing"],
    "in_shutdown_processing": ["inactive"],
    "inactive": []
}

ALLOWED_STATES = json.loads(os.getenv('ALLOWED_STATES'))

def can_transition(current_state: str, new_state: str) -> bool:
    if current_state not in ALLOWED_STATES or new_state not in ALLOWED_STATES:
        return False
    return new_state in TRANSITIONS.get(current_state, [])
