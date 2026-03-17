#!/bin/bash
# Total.kz Dashboard — VPS Setup Script
# Usage: curl -sL https://raw.githubusercontent.com/belilovsky/total-kz/main/setup.sh | bash
set -euo pipefail

REPO="https://github.com/belilovsky/total-kz.git"
APP_DIR="/opt/total-kz"
DOMAIN=""  # Set your domain here if needed

echo "=== Total.kz Dashboard Setup ==="

# Install Docker if not present
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
fi

# Install Docker Compose plugin if not present
if ! docker compose version &> /dev/null; then
    echo "Installing Docker Compose plugin..."
    apt-get update && apt-get install -y docker-compose-plugin
fi

# Clone or update repo
if [ -d "$APP_DIR" ]; then
    echo "Updating existing installation..."
    cd "$APP_DIR"
    git pull origin main
else
    echo "Cloning repository..."
    git clone "$REPO" "$APP_DIR"
    cd "$APP_DIR"
fi

# Create data directory
mkdir -p data

# Build and start
echo "Building and starting containers..."
docker compose down --remove-orphans 2>/dev/null || true
docker compose up -d --build

# Wait for health check
echo "Waiting for service to start..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8000/api/stats > /dev/null 2>&1; then
        echo "Service is running!"
        break
    fi
    sleep 2
done

# Setup Nginx reverse proxy if nginx is installed
if command -v nginx &> /dev/null && [ -n "$DOMAIN" ]; then
    echo "Configuring Nginx for $DOMAIN..."
    cat > /etc/nginx/sites-available/total-kz <<EOF
server {
    listen 80;
    server_name $DOMAIN;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF
    ln -sf /etc/nginx/sites-available/total-kz /etc/nginx/sites-enabled/
    nginx -t && systemctl reload nginx
fi

echo ""
echo "=== Setup Complete ==="
echo "Dashboard: http://$(hostname -I | awk '{print $1}'):8000"
echo ""
echo "Next steps:"
echo "  1. Place articles.jsonl in $APP_DIR/data/"
echo "  2. Import: docker compose exec web python scraper/import_data.py"
echo "  3. (Optional) Run scraper: docker compose exec web python scraper/scrape_urls.py"
echo ""
