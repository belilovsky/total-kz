#!/bin/bash
# ═══════════════════════════════════════════════════════
# Нагрузочное тестирование total.kz
# Использует Apache Bench (ab) — предустановлен в большинстве Linux
#
# Запуск:
#   ./scripts/loadtest.sh                    # тест prod (total.qdev.run)
#   ./scripts/loadtest.sh http://localhost:3847  # тест локально
#   ./scripts/loadtest.sh https://total.qdev.run full  # полный тест
# ═══════════════════════════════════════════════════════

set -e

BASE_URL="${1:-https://total.qdev.run}"
MODE="${2:-quick}"  # quick | full

# Убираем trailing slash
BASE_URL="${BASE_URL%/}"

echo "═══════════════════════════════════════════════════"
echo "  НАГРУЗОЧНОЕ ТЕСТИРОВАНИЕ: $BASE_URL"
echo "  Режим: $MODE"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "═══════════════════════════════════════════════════"

# ── Параметры ──
if [ "$MODE" = "full" ]; then
    REQUESTS=500
    CONCURRENCY=50
else
    REQUESTS=100
    CONCURRENCY=10
fi

RESULTS_DIR="data/loadtest_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

run_test() {
    local name="$1"
    local path="$2"
    local url="${BASE_URL}${path}"

    echo ""
    echo "── $name ──"
    echo "   URL: $url"
    echo "   Запросы: $REQUESTS, Параллельность: $CONCURRENCY"

    # Проверяем доступность
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" = "000" ]; then
        echo "   ⚠ Недоступен, пропускаю"
        return
    fi
    echo "   HTTP: $HTTP_CODE"

    ab -n $REQUESTS -c $CONCURRENCY -q \
       -H "Accept-Encoding: gzip,deflate" \
       "$url" > "$RESULTS_DIR/${name}.txt" 2>&1

    # Извлекаем ключевые метрики
    RPS=$(grep "Requests per second" "$RESULTS_DIR/${name}.txt" | awk '{print $4}')
    MEAN=$(grep "Time per request.*mean\)" "$RESULTS_DIR/${name}.txt" | head -1 | awk '{print $4}')
    P50=$(grep "50%" "$RESULTS_DIR/${name}.txt" | awk '{print $2}')
    P95=$(grep "95%" "$RESULTS_DIR/${name}.txt" | awk '{print $2}')
    P99=$(grep "99%" "$RESULTS_DIR/${name}.txt" | awk '{print $2}')
    FAILED=$(grep "Failed requests" "$RESULTS_DIR/${name}.txt" | awk '{print $3}')

    echo "   RPS: $RPS | Mean: ${MEAN}ms | P50: ${P50}ms | P95: ${P95}ms | P99: ${P99}ms | Ошибки: $FAILED"

    # Записываем в сводку
    printf "%-25s %8s %8s %8s %8s %8s %6s\n" "$name" "$RPS" "${MEAN}" "${P50}" "${P95}" "${P99}" "$FAILED" >> "$RESULTS_DIR/summary.txt"
}

# Заголовок сводки
printf "%-25s %8s %8s %8s %8s %8s %6s\n" "Страница" "RPS" "Mean(ms)" "P50(ms)" "P95(ms)" "P99(ms)" "Ошибки" > "$RESULTS_DIR/summary.txt"
printf "%-25s %8s %8s %8s %8s %8s %6s\n" "─────────────────────────" "────────" "────────" "────────" "────────" "────────" "──────" >> "$RESULTS_DIR/summary.txt"

# ── Тесты ──
run_test "homepage"        "/"
run_test "category"        "/news/obshchestvo"
run_test "article"         "/news/obshchestvo/poterpevshie_prosyat_nakazat_perizat_kairat_po_vsei_strogosti_zakona_date_2026_03_22_10_02_18"
run_test "search"          "/search?q=Токаев"
run_test "persons"         "/persons"
run_test "api_health"      "/health"
run_test "static_css"      "/static/css/public.css"

if [ "$MODE" = "full" ]; then
    run_test "category_2"    "/news/politika"
    run_test "category_3"    "/news/ekonomika"
    run_test "tag"           "/tag/Токаев"
    run_test "search_long"   "/search?q=экономика+Казахстан"
    run_test "person"        "/person/tokayev-kasym-zhomart"
    run_test "api_articles"  "/api/search/articles?q=нефть"
    run_test "sitemap"       "/sitemap.xml"
fi

echo ""
echo "═══════════════════════════════════════════════════"
echo "  СВОДКА"
echo "═══════════════════════════════════════════════════"
cat "$RESULTS_DIR/summary.txt"
echo ""
echo "Полные результаты: $RESULTS_DIR/"
echo ""

# ── Анализ ──
echo "── РЕКОМЕНДАЦИИ ──"
echo ""

# Проверяем средний RPS
AVG_RPS=$(awk 'NR>2 && $2+0>0 {sum+=$2; n++} END {if(n>0) printf "%.1f", sum/n; else print "0"}' "$RESULTS_DIR/summary.txt")
echo "Средний RPS: $AVG_RPS"

if (( $(echo "$AVG_RPS > 100" | bc -l 2>/dev/null || echo 0) )); then
    echo "✅ Производительность отличная (>100 RPS)"
elif (( $(echo "$AVG_RPS > 30" | bc -l 2>/dev/null || echo 0) )); then
    echo "⚠ Производительность нормальная (30-100 RPS)"
    echo "   Рекомендации: включить кеширование, оптимизировать SQL-запросы"
else
    echo "❌ Производительность низкая (<30 RPS)"
    echo "   Рекомендации:"
    echo "   - Включить Redis для кеширования страниц"
    echo "   - Оптимизировать тяжёлые SQL-запросы"
    echo "   - Проверить N+1 проблемы в ORM"
    echo "   - Увеличить воркеры uvicorn (--workers 4)"
fi
