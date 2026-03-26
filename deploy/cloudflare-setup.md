# Настройка Cloudflare для total.kz

## 1. Регистрация и добавление сайта

1. Зарегистрируйтесь на [cloudflare.com](https://www.cloudflare.com/)
2. Нажмите **Add a Site** → введите `total.kz`
3. Выберите план **Free** (достаточно для начала)
4. Cloudflare покажет текущие DNS-записи — проверьте их

## 2. Смена DNS (nameservers)

1. Cloudflare выдаст два nameserver'а, например:
   - `anna.ns.cloudflare.com`
   - `bob.ns.cloudflare.com`
2. Зайдите в панель регистратора домена `total.kz`
3. Замените текущие NS-записи на Cloudflare nameservers
4. Ожидайте до 24 часов на пропагацию (обычно 1-2 часа)

## 3. Настройка DNS-записей

В разделе **DNS** → **Records** добавьте:

| Тип   | Имя       | Значение       | Прокси  |
|-------|-----------|----------------|---------|
| A     | total.kz  | 62.72.32.112   | Proxied |
| A     | www       | 62.72.32.112   | Proxied |

**Важно**: оранжевое облако (Proxied) должно быть включено для защиты.

## 4. SSL/TLS

1. Перейдите в **SSL/TLS** → **Overview**
2. Установите режим: **Full (Strict)**
   - Требует валидный SSL-сертификат на сервере (Let's Encrypt)
3. В **Edge Certificates**:
   - **Always Use HTTPS**: включить
   - **Minimum TLS Version**: TLS 1.2
   - **Automatic HTTPS Rewrites**: включить
   - **HSTS**: включить (max-age 6 месяцев, includeSubDomains)

## 5. Кэширование

1. **Caching** → **Configuration**:
   - **Caching Level**: Standard
   - **Browser Cache TTL**: Respect Existing Headers
2. **Cache Rules** (создайте правила):
   - `/static/*` → Cache Everything, Edge TTL 30 дней
   - `/imgproxy/*` → Cache Everything, Edge TTL 30 дней
   - `/` (главная) → Edge TTL 10 секунд (микрокеш)
   - `/category/*` → Edge TTL 1 минута

## 6. Firewall / WAF

1. **Security** → **WAF**:
   - Включите **Managed Rules** (OWASP Core Rule Set)
   - Установите чувствительность на **Medium**
2. **Security** → **Bots**:
   - Включите **Bot Fight Mode** (бесплатно)
3. Создайте **Custom Rules**:
   - Блокировать запросы из стран, которые не являются целевой аудиторией
     (оставьте KZ, RU, BY, UA, DE, US)
   - Блокировать запросы без User-Agent

## 7. Rate Limiting (бесплатный план)

1. **Security** → **WAF** → **Rate limiting rules**
2. Создайте правило:
   - URL: `*total.kz/admin/*`
   - Порог: 10 запросов за 1 минуту
   - Действие: Block на 1 час
3. Ещё одно правило:
   - URL: `*total.kz/*`
   - Порог: 100 запросов за 10 секунд
   - Действие: Challenge на 10 минут

## 8. Page Rules (3 бесплатных)

1. `*total.kz/admin/*` → SSL: Full Strict, Security Level: High, Cache Level: Bypass
2. `*total.kz/static/*` → Cache Level: Cache Everything, Edge TTL: 30 days
3. `*total.kz/imgproxy/*` → Cache Level: Cache Everything, Edge TTL: 30 days

## 9. Активация Real IP в Nginx

После включения Cloudflare, раскомментируйте секцию в `deploy/nginx/total.conf`:

```nginx
set_real_ip_from 173.245.48.0/20;
# ... (все диапазоны Cloudflare)
real_ip_header CF-Connecting-IP;
```

Затем перезапустите nginx:
```bash
nginx -t && systemctl reload nginx
```

## 10. Проверка

1. Проверьте DNS: `dig total.kz +short` — должен показать IP Cloudflare, не сервера
2. Проверьте SSL: `curl -I https://total.kz` — должен содержать `cf-ray` заголовок
3. Проверьте WAF: попробуйте SQL-инъекцию в URL — должна быть заблокирована
4. Проверьте кэш: `curl -I https://total.kz/static/css/main.css` — должен содержать `cf-cache-status: HIT`

## Полезные ссылки

- [Cloudflare Docs](https://developers.cloudflare.com/)
- [Список IP-диапазонов Cloudflare](https://www.cloudflare.com/ips/)
- [Настройка SSL](https://developers.cloudflare.com/ssl/)
