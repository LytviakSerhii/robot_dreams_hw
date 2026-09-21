# Завдання 1-2: чиста логіка перетворення (без Kafka, без мережі).
# Реалізуйте обидві функції. Їх перевіряє tests/test_transform.py.
from datetime import datetime

# Дано, не редагувати. П'ять типів подій, які лишаємо; решта відкидається.
ALLOWED_TYPES = (
    "PushEvent",
    "IssuesEvent",
    "PullRequestEvent",
    "WatchEvent",
    "IssueCommentEvent",
)


def event_filter(event: dict) -> bool:
    return (
        event.get("type") in ALLOWED_TYPES
        and event.get("public", True) is True
    )


def flatten_event(event: dict) -> dict:
    payload = event.get("payload") or {}
    commits = payload.get("commits")
    payload_commit_count = len(commits) if commits is not None else None

    return {
        "id": str(event["id"]),
        "event_type": str(event["type"]),
        "created_at": _to_millis(event["created_at"]),
        "actor_login": str(event["actor"]["login"]),
        "repo_name": str(event["repo"]["name"]),
        "public": bool(event.get("public", True)),
        "payload_action": payload.get("action"),
        "payload_ref": payload.get("ref"),
        "payload_commit_count": payload_commit_count,
    }

def _to_millis(created_at: str) -> int:
    """ISO-8601 ('2024-01-15T14:00:01Z') -> epoch-мілісекунди (int). Готова функція."""
    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)
