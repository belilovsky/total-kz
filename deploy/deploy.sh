#!/bin/bash
# total.kz — One-command production deployment
# Usage: sudo bash deploy/deploy.sh
#
# This script:
# 1. Pulls latest code
# 2. Rebuilds Docker containers
# 3. Installs nginx config
# 4. Sets up firewall
# 5. Configures fail2ban

set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$DEPLOY_DIR")"
DOMAIN="total.kz"

echo "=== total.kz Production Deployment ==="
echo "Project: $PROJECT_DIR"
echo ""

# ── 1. Pull latest code ──
echo "→ Pulling latest code..."
cd "$PROJECT_DIR"
git pull origin main

# ── 2. Rebuild containers ──
echo "→ Rebuilding Docker containers..."
docker compose up -d --build

# ── 3. Wait for health check ──
echo "→ Waiting for app to start..."
for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:3847/health > /dev/null 2>&1; then
        echo "  App is healthy!"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  WARNING: App health check failed after 30 attempts"
        docker compose logs app --tail=20
    fi
    sleep 2
done

# ── 4. Install nginx config ──
echo "→ Installing nginx config..."
if [ -f "$DEPLOY_DIR/nginx/total.conf" ]; then
    cp "$DEPLOY_DIR/nginx/total.conf" /etc/nginx/sites-available/total.conf
    ln -sf /etc/nginx/sites-available/total.conf /etc/nginx/sites-enabled/total.conf

    # Create cache directory
    mkdir -p /var/cache/nginx/total

    # Test and reload
    nginx -t && systemctl reload nginx
    echo "  Nginx configured and reloaded"
else
    echo "  WARNING: nginx config not found, skipping"
fi

# ── 5. Setup firewall ──
echo "→ Setting up firewall..."
if [ -f "$DEPLOY_DIR/setup-firewall.sh" ]; then
    bash "$DEPLOY_DIR/setup-firewall.sh"
else
    echo "  WARNING: firewall script not found, skipping"
fi

# ── 6. Setup fail2ban ──
echo "→ Configuring fail2ban..."
if command -v fail2ban-server > /dev/null 2>&1; then
    cp "$DEPLOY_DIR/fail2ban/jail.local" /etc/fail2ban/jail.local
    cp "$DEPLOY_DIR/fail2ban/filter.d/"*.conf /etc/fail2ban/filter.d/
    systemctl restart fail2ban
    echo "  fail2ban configured and restarted"
else
    echo "  Installing fail2ban..."
    apt-get update -qq && apt-get install -y -qq fail2ban
    cp "$DEPLOY_DIR/fail2ban/jail.local" /etc/fail2ban/jail.local
    cp "$DEPLOY_DIR/fail2ban/filter.d/"*.conf /etc/fail2ban/filter.d/
    systemctl enable fail2ban
    systemctl start fail2ban
    echo "  fail2ban installed and started"
fi

# ── 7. Summary ──
echo ""
echo "=== Deployment Complete ==="
echo "  Docker:    $(docker compose ps --format 'table {{.Name}}\t{{.Status}}' 2>/dev/null | tail -n +2 | wc -l) containers running"
echo "  Nginx:     $(systemctl is-active nginx)"
echo "  Firewall:  $(ufw status | head -1)"
echo "  fail2ban:  $(systemctl is-active fail2ban)"
echo ""
echo "Next steps:"
echo "  1. Setup SSL: certbot --nginx -d $DOMAIN -d www.$DOMAIN"
echo "  2. Setup Cloudflare: see deploy/cloudflare-setup.md"
echo "  3. Verify: curl -I https://$DOMAIN"
