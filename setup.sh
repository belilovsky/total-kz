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

# Create data directory and reassemble + unpack articles
mkdir -p data
if [ ! -f "data/articles.jsonl" ]; then
    if ls data/articles.jsonl.gz.part_* 1>/dev/null 2>&1; then
        echo "Reassembling articles archive..."
        cat data/articles.jsonl.gz.part_* > data/articles.jsonl.gz
        echo "Unpacking articles data..."
        gunzip -f data/articles.jsonl.gz
    fi
fi

# Build and start
echo "Building and starting containers..."
docker compose down --remove-orphans 2>/dev/null || true
docker compose up -d --build

# Wait for health check
echo "Waiting for service to start..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:3847/api/stats > /dev/null 2>&1; then
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
        proxy_pass http://127.0.0.1:3847;
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
echo "Dashboard: http://$(hostname -I | awk '{print $1}'):3847"
echo ""
# Auto-import if data exists
if [ -f "data/articles.jsonl" ]; then
    echo "Importing articles into database..."
    docker compose exec -T web python scraper/import_data.py
fi

echo "Next steps:"
echo "  (Optional) Run scraper: docker compose exec web python scraper/scrape_urls.py"
echo ""
