#!/bin/bash
# Setup Nginx reverse proxy for total.qdev.run
# Run as root on VPS: bash /opt/total-kz/deploy/setup-nginx.sh

set -e

DOMAIN="total.qdev.run"
CONF_SRC="/opt/total-kz/deploy/nginx-total-qdev-run.conf"
CONF_DST="/etc/nginx/sites-available/$DOMAIN"

echo "=== 1. Installing Nginx + Certbot ==="
apt update
apt install -y nginx certbot python3-certbot-nginx

echo "=== 2. Copying Nginx config ==="
cp "$CONF_SRC" "$CONF_DST"

# Enable site
ln -sf "$CONF_DST" /etc/nginx/sites-enabled/

# Remove default if exists
rm -f /etc/nginx/sites-enabled/default

echo "=== 3. Testing Nginx config ==="
nginx -t

echo "=== 4. Getting SSL certificate ==="
# First, start nginx with HTTP only for certbot validation
# Temporarily comment out SSL lines
sed -i 's/listen 443 ssl http2;/# listen 443 ssl http2;/' "$CONF_DST"
sed -i 's/ssl_certificate/# ssl_certificate/' "$CONF_DST"
sed -i 's/include \/etc\/letsencrypt/# include \/etc\/letsencrypt/' "$CONF_DST"
sed -i 's/ssl_dhparam/# ssl_dhparam/' "$CONF_DST"

# Temp: make HTTP server proxy to app (not redirect)
cat > /tmp/nginx-temp-http.conf <<'EOF'
server {
    listen 80;
    server_name total.qdev.run;
    location / {
        proxy_pass http://127.0.0.1:3847;
    }
}
EOF
cp /tmp/nginx-temp-http.conf "$CONF_DST"
nginx -t && systemctl reload nginx

# Get cert
certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --email a.belilovsky@gmail.com --redirect

echo "=== 5. Restoring full config ==="
cp "$CONF_SRC" "$CONF_DST"
nginx -t && systemctl reload nginx

echo ""
echo "✅ Done! https://$DOMAIN should now be live."
echo ""
echo "Next steps:"
echo "  1. Fix imgproxy port conflict:"
echo "     docker compose down imgproxy && docker compose up -d imgproxy"
echo "  2. Reindex Meilisearch:"
echo "     docker compose exec app python -m scraper.reindex_meilisearch"
echo "  3. Setup Umami: visit https://$DOMAIN/umami/"
echo "     Login: admin / umami"
