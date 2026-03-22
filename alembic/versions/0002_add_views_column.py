"""Add views column to articles.

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-22
"""

from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("articles", sa.Column("views", sa.Integer(), server_default="0"))


def downgrade() -> None:
    op.drop_column("articles", "views")
