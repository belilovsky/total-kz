"""Editorial workflow engine — role-based article status transitions.

Inspired by Legend Workflow (7-stage ticket lifecycle),
adapted for Total.kz CMS with 5 article statuses.
"""

from . import database as db

# ── Status definitions ──────────────────────────────────────────────

STATUSES = {
    "draft":     {"label": "Черновик",              "color": "#6b7280"},
    "review":    {"label": "На рецензии",           "color": "#f59e0b"},
    "ready":     {"label": "Готово к публикации",   "color": "#3b82f6"},
    "published": {"label": "Опубликовано",          "color": "#22c55e"},
    "archived":  {"label": "В архиве",              "color": "#9ca3af"},
}

STATUS_LABELS = {k: v["label"] for k, v in STATUSES.items()}

# ── Transition definitions ──────────────────────────────────────────
# Each action: (from_status, to_status, allowed_roles, label_ru)

TRANSITIONS = {
    "submit_review":   ("draft",     "review",    ["journalist", "editor", "admin"], "Отправить на рецензию"),
    "withdraw":        ("review",    "draft",     ["journalist", "editor", "admin"], "Отозвать"),
    "approve":         ("review",    "ready",     ["editor", "admin"],               "Одобрить"),
    "request_changes": ("review",    "draft",     ["editor", "admin"],               "Запросить правки"),
    "publish":         ("ready",     "published", ["editor", "admin"],               "Опубликовать"),
    "unpublish":       ("published", "draft",     ["admin"],                         "Снять с публикации"),
    "archive":         ("published", "archived",  ["editor", "admin"],               "В архив"),
    "unarchive":       ("archived",  "draft",     ["admin"],                         "Из архива"),
}


class WorkflowError(Exception):
    """Base workflow exception."""


class InvalidStateError(WorkflowError):
    """Article is not in the required state for this action."""


class PermissionDeniedError(WorkflowError):
    """User role is not allowed to perform this action."""


def get_available_actions(status: str, role: str) -> list[dict]:
    """Return list of actions available for the given status and role.

    Each item: {"action": str, "label": str, "to_status": str}
    """
    actions = []
    for action_name, (from_st, to_st, roles, label) in TRANSITIONS.items():
        if from_st == status and role in roles:
            actions.append({
                "action": action_name,
                "label": label,
                "to_status": to_st,
            })
    return actions


def execute_transition(
    article_id: int,
    action: str,
    user: dict,
    comment: str = "",
) -> dict:
    """Execute a workflow transition on an article.

    Args:
        article_id: Article ID.
        action: Transition action name (e.g. "approve").
        user: Current user dict with keys: user_id, username, role.
        comment: Optional comment to attach.

    Returns:
        {"ok": True, "new_status": str, "message": str}

    Raises:
        InvalidStateError: If article is not in the expected state.
        PermissionDeniedError: If user role is not allowed.
        WorkflowError: If action is unknown or article not found.
    """
    if action not in TRANSITIONS:
        raise WorkflowError(f"Неизвестное действие: {action}")

    from_status, to_status, allowed_roles, label = TRANSITIONS[action]
    role = user.get("role", "")
    username = user.get("username", "")
    user_id = user.get("user_id", 0)

    # Fetch article
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT id, status, title FROM articles WHERE id = ?",
            (article_id,),
        ).fetchone()

    if not row:
        raise WorkflowError("Статья не найдена")

    current_status = row["status"] or "published"

    # Validate state
    if current_status != from_status:
        expected = STATUS_LABELS.get(from_status, from_status)
        actual = STATUS_LABELS.get(current_status, current_status)
        raise InvalidStateError(
            f"Статья должна быть в статусе «{expected}», "
            f"текущий статус: «{actual}»"
        )

    # Validate role
    if role not in allowed_roles:
        raise PermissionDeniedError(
            f"Роль «{role}» не может выполнить действие «{label}»"
        )

    # Execute transition
    with db.get_db() as conn:
        conn.execute(
            "UPDATE articles SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (to_status, article_id),
        )

    # Audit log
    details = f"{label}: {STATUS_LABELS.get(from_status, from_status)} → {STATUS_LABELS.get(to_status, to_status)}"
    if comment:
        details += f" | {comment[:100]}"
    db.log_audit(
        user_id=user_id,
        username=username,
        action="workflow",
        entity_type="article",
        entity_id=article_id,
        details=details[:200],
    )

    # Save comment if provided
    if comment and comment.strip():
        add_comment(article_id, user_id, username, role, comment.strip())

    new_label = STATUS_LABELS.get(to_status, to_status)
    return {
        "ok": True,
        "new_status": to_status,
        "message": f"{label} — статус изменён на «{new_label}»",
    }


def assign_article(article_id: int, assigned_to: str, user: dict) -> dict:
    """Assign an article to a journalist.

    Only editors and admins can assign.
    """
    role = user.get("role", "")
    if role not in ("editor", "admin"):
        raise PermissionDeniedError("Только редактор или администратор может назначить статью")

    with db.get_db() as conn:
        row = conn.execute("SELECT id FROM articles WHERE id = ?", (article_id,)).fetchone()
        if not row:
            raise WorkflowError("Статья не найдена")
        conn.execute(
            "UPDATE articles SET assigned_to = ?, updated_at = datetime('now') WHERE id = ?",
            (assigned_to or None, article_id),
        )

    detail = f"Назначено: {assigned_to}" if assigned_to else "Назначение снято"
    db.log_audit(
        user_id=user.get("user_id", 0),
        username=user.get("username", ""),
        action="assign",
        entity_type="article",
        entity_id=article_id,
        details=detail[:200],
    )

    return {"ok": True, "assigned_to": assigned_to, "message": detail}


# ── Comments ────────────────────────────────────────────────────────

def add_comment(article_id: int, user_id: int, username: str, role: str, comment: str) -> dict:
    """Add a comment to an article. Returns the created comment dict."""
    with db.get_db() as conn:
        cur = conn.execute(
            """INSERT INTO article_comments (article_id, user_id, username, role, comment)
               VALUES (?, ?, ?, ?, ?)""",
            (article_id, user_id, username, role, comment),
        )
        comment_id = cur.lastrowid
        row = conn.execute(
            "SELECT * FROM article_comments WHERE id = ?", (comment_id,)
        ).fetchone()

    return dict(row) if row else {"id": comment_id}


def get_comments(article_id: int) -> list[dict]:
    """Get all comments for an article, newest first."""
    with db.get_db() as conn:
        rows = conn.execute(
            """SELECT id, article_id, user_id, username, role, comment, created_at
               FROM article_comments WHERE article_id = ? ORDER BY created_at ASC""",
            (article_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_workflow_history(article_id: int) -> list[dict]:
    """Get workflow-related audit log entries for an article."""
    with db.get_db() as conn:
        rows = conn.execute(
            """SELECT id, username, action, details, created_at
               FROM audit_log
               WHERE entity_type = 'article' AND entity_id = ?
                 AND action IN ('workflow', 'assign', 'publish', 'create', 'update')
               ORDER BY created_at DESC
               LIMIT 50""",
            (article_id,),
        ).fetchall()
    return [dict(r) for r in rows]
