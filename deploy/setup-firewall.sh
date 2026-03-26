#!/bin/bash
# total.kz — UFW Firewall Setup
# Run: sudo bash deploy/setup-firewall.sh

set -euo pipefail

echo "=== Setting up UFW firewall ==="

# Reset to defaults
ufw --force reset

# Default policies
ufw default deny incoming
ufw default allow outgoing

# Allow SSH
ufw allow 22/tcp comment 'SSH'

# Allow HTTP
ufw allow 80/tcp comment 'HTTP'

# Allow HTTPS
ufw allow 443/tcp comment 'HTTPS'

# Enable firewall
ufw --force enable

echo "=== Firewall configured ==="
ufw status verbose
