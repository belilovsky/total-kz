#!/bin/bash
# Download total.db from GitHub Release and deploy to Docker
# Run on VPS: bash scripts/download_db_from_github.sh

set -e

TOKEN="${GITHUB_TOKEN:?Укажите GITHUB_TOKEN}"
REPO="belilovsky/total-kz"
RELEASE_TAG="db-sync-2026-03-22"
DB_DIR="/opt/total-kz/data"
TEMP_DIR="/tmp/db-download"

echo "=== Скачивание базы данных total.db из GitHub ==="
echo ""

# Create temp dir
mkdir -p "$TEMP_DIR"
cd "$TEMP_DIR"

# Get release info
echo "1. Получаем информацию о релизе..."
RELEASE_INFO=$(curl -s -H "Authorization: token $TOKEN" \
  "https://api.github.com/repos/$REPO/releases/tags/$RELEASE_TAG")

# Extract asset IDs and names
echo "$RELEASE_INFO" | python3 -c "
import sys, json
data = json.load(sys.stdin)
assets = data.get('assets', [])
print(f'Найдено {len(assets)} файлов')
for a in sorted(assets, key=lambda x: x['name']):
    print(f'  {a[\"name\"]}: {a[\"size\"]/1024/1024:.1f} MB')
" 2>/dev/null || echo "Ошибка получения информации о релизе!"

# Download all parts
echo ""
echo "2. Скачиваем части..."
for PART in aa ab ac; do
    FILENAME="total.db.gz.part-${PART}"
    ASSET_URL=$(echo "$RELEASE_INFO" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for a in data.get('assets', []):
    if a['name'] == '$FILENAME':
        print(a['url'])
        break
")
    
    if [ -z "$ASSET_URL" ]; then
        echo "  Пропуск $FILENAME (не найден)"
        continue
    fi
    
    echo "  Скачиваем $FILENAME..."
    curl -L -s -H "Authorization: token $TOKEN" \
         -H "Accept: application/octet-stream" \
         "$ASSET_URL" -o "$FILENAME"
    ls -lh "$FILENAME"
done

# Combine and decompress
echo ""
echo "3. Собираем и распаковываем..."
cat total.db.gz.part-* > total.db.gz
ls -lh total.db.gz
echo "  Распаковка..."
gunzip total.db.gz
ls -lh total.db

# Backup existing DB
echo ""
echo "4. Бэкап текущей базы..."
if [ -f "$DB_DIR/total.db" ]; then
    BACKUP="$DB_DIR/total_backup_$(date +%Y%m%d_%H%M%S).db"
    cp "$DB_DIR/total.db" "$BACKUP"
    echo "  Бэкап: $BACKUP"
else
    echo "  Текущая база не найдена, пропускаем бэкап"
fi

# Replace DB
echo ""
echo "5. Замена базы данных..."
cp total.db "$DB_DIR/total.db"
echo "  Готово: $(ls -lh $DB_DIR/total.db)"

# Rebuild Docker
echo ""
echo "6. Перезапуск Docker..."
cd /opt/total-kz
docker compose build app
docker compose up -d app

echo ""
echo "=== Готово! ==="
echo "База данных обновлена. Проверьте: https://total.qdev.run"

# Cleanup
rm -rf "$TEMP_DIR"
