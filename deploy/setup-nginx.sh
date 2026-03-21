#!/bin/bash
# Setup Nginx reverse proxy for total.qdev.run
# Run as root on VPS: bash /opt/total-kz/deploy/setup-nginx.sh
set -e

DOMAIN="total.qdev.run"
EMAIL="a.belilovsky@gmail.com"
CONF_FINAL="/opt/total-kz/deploy/nginx-total-qdev-run.conf"
CONF_DST="/etc/nginx/sites-available/$DOMAIN"

echo "=== 1. Installing Nginx + Certbot ==="
apt update -qq
apt install -y nginx certbot python3-certbot-nginx

echo "=== 2. Creating temporary HTTP-only config ==="
cat > "$CONF_DST" <<'HTTPCONF'
server {
    listen 80;
    server_name total.qdev.run;

    location / {
        proxy_pass http://127.0.0.1:3847;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        client_max_body_size 50M;
    }
}
HTTPCONF

ln -sf "$CONF_DST" /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default

echo "=== 3. Testing & reloading Nginx (HTTP-only) ==="
nginx -t && systemctl reload nginx
echo "  ✓ http://$DOMAIN should be reachable now"

echo "=== 4. Getting SSL certificate via Certbot ==="
certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --email "$EMAIL" --redirect
echo "  ✓ SSL certificate obtained"

echo "=== 5. Installing full config with all locations ==="
cp "$CONF_FINAL" "$CONF_DST"
nginx -t && systemctl reload nginx

echo ""
echo "✅ Done! https://$DOMAIN is live."
echo ""
echo "Umami dashboard: https://$DOMAIN/umami/ (login: admin / umami)"
