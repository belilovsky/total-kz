/* Total.kz Dashboard – v4.0 (enhanced toasts, notifications, keyboard shortcuts) */
document.addEventListener('DOMContentLoaded', function() {

    // ===== ENHANCED TOAST SYSTEM =====
    window.showToast = function(msg, type) {
        // type: 'success' (default), 'error', 'info'
        if (!type) {
            // Auto-detect type from message content
            if (msg.toLowerCase().indexOf('ошибка') !== -1 || msg.toLowerCase().indexOf('error') !== -1) type = 'error';
            else if (msg.toLowerCase().indexOf('инфо') !== -1) type = 'info';
            else type = 'success';
        }
        var container = document.getElementById('toastContainer');
        if (!container) {
            // Fallback to legacy toast
            var t = document.getElementById('toast');
            if (t) { t.textContent = msg; t.classList.add('show'); setTimeout(function(){ t.classList.remove('show'); }, 2500); }
            return;
        }

        var icons = {
            success: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="toast-icon"><polyline points="20 6 9 17 4 12"/></svg>',
            error: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="toast-icon"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
            info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="toast-icon"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg>'
        };

        var el = document.createElement('div');
        el.className = 'toast-item toast-' + type;
        el.innerHTML = icons[type] + '<span class="toast-msg">' + msg + '</span><span class="toast-close" onclick="this.parentElement.classList.add(\'hiding\');setTimeout(function(){el.remove()},350)">&times;</span>';
        container.appendChild(el);

        // Trigger animation
        requestAnimationFrame(function() { el.classList.add('show'); });

        // Auto-dismiss
        var duration = type === 'error' ? 8000 : type === 'info' ? 5000 : 3000;
        var timer = setTimeout(function() {
            el.classList.remove('show');
            el.classList.add('hiding');
            setTimeout(function() { if (el.parentNode) el.remove(); }, 350);
        }, duration);

        // Click to dismiss
        el.addEventListener('click', function() {
            clearTimeout(timer);
            el.classList.remove('show');
            el.classList.add('hiding');
            setTimeout(function() { if (el.parentNode) el.remove(); }, 350);
        });

        // Limit to 5 visible toasts
        var items = container.querySelectorAll('.toast-item');
        if (items.length > 5) items[0].remove();
    };


    // ===== NOTIFICATIONS PANEL =====
    window.toggleNotifPanel = function() {
        var dd = document.getElementById('notifDropdown');
        if (dd) dd.classList.toggle('open');
    };

    window.markRead = function(el) {
        el.classList.remove('unread');
        updateNotifBadge();
    };

    window.markAllRead = function() {
        document.querySelectorAll('.notif-item.unread').forEach(function(item) {
            item.classList.remove('unread');
        });
        updateNotifBadge();
        // Persist read state
        localStorage.setItem('notif_read_at', Date.now().toString());
    };

    function updateNotifBadge() {
        var badge = document.getElementById('notifBadge');
        if (!badge) return;
        var count = document.querySelectorAll('.notif-item.unread').length;
        badge.textContent = count > 0 ? count : '';
        badge.setAttribute('data-count', count);
    }

    // Close notifications on outside click
    document.addEventListener('click', function(e) {
        var dd = document.getElementById('notifDropdown');
        if (dd && dd.classList.contains('open') && !e.target.closest('.notif-bell-wrap')) {
            dd.classList.remove('open');
        }
    });

    // Check if notifications were already read
    (function() {
        var readAt = localStorage.getItem('notif_read_at');
        if (readAt) {
            // If read within last 5 minutes, mark all as read
            var diff = Date.now() - parseInt(readAt);
            if (diff < 5 * 60 * 1000) {
                document.querySelectorAll('.notif-item.unread').forEach(function(item) {
                    item.classList.remove('unread');
                });
                updateNotifBadge();
            }
        }
    })();


    // ===== THEME TOGGLE =====
    const themeToggle = document.getElementById('themeToggle');
    if (themeToggle) {
        themeToggle.addEventListener('click', function() {
            const html = document.documentElement;
            const current = html.getAttribute('data-theme') || 'light';
            const next = current === 'dark' ? 'light' : 'dark';
            html.setAttribute('data-theme', next);
            localStorage.setItem('theme', next);
            updateChartColors(next);
        });
    }

    // Update Chart.js colors on theme change
    window.updateChartColors = function(theme) {
        if (typeof Chart === 'undefined') return;
        const isDark = theme === 'dark';
        Chart.defaults.color = isDark ? '#9ca3af' : '#6b7280';
        Chart.defaults.borderColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.04)';

        Chart.helpers.each(Chart.instances, function(chart) {
            if (!chart) return;
            const opts = chart.options;
            if (opts.plugins && opts.plugins.tooltip) {
                opts.plugins.tooltip.backgroundColor = isDark ? '#2a2d36' : '#1a1d26';
                opts.plugins.tooltip.titleColor = isDark ? '#e8eaed' : '#ffffff';
                opts.plugins.tooltip.bodyColor = isDark ? '#e8eaed' : '#ffffff';
            }
            if (opts.scales) {
                Object.keys(opts.scales).forEach(function(key) {
                    var scale = opts.scales[key];
                    if (scale.grid) scale.grid.color = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.04)';
                    if (scale.ticks) scale.ticks.color = isDark ? '#9ca3af' : '#6b7280';
                });
            }
            chart.update('none');
        });
    };

    // Apply chart colors on load
    var savedTheme = localStorage.getItem('theme') || 'light';
    if (savedTheme === 'dark' && typeof Chart !== 'undefined') {
        Chart.defaults.color = '#9ca3af';
        Chart.defaults.borderColor = 'rgba(255,255,255,0.06)';
    }


    // ===== KEYBOARD SHORTCUTS =====
    document.addEventListener('keydown', function(e) {
        var isInput = e.target.matches('input, textarea, select, [contenteditable]');

        // Escape — close modals
        if (e.key === 'Escape') {
            // Close keyboard shortcuts modal
            var sm = document.getElementById('shortcutsModal');
            if (sm && sm.classList.contains('open')) { sm.classList.remove('open'); return; }
            // Close notifications
            var nd = document.getElementById('notifDropdown');
            if (nd && nd.classList.contains('open')) { nd.classList.remove('open'); return; }
            // Close preview modal
            var pm = document.getElementById('previewModal');
            if (pm && pm.style.display !== 'none' && pm.style.display !== '') {
                pm.style.display = 'none'; return;
            }
            return;
        }

        // ? key — show shortcuts help (only when not in input)
        if (e.key === '?' && !isInput) {
            e.preventDefault();
            var modal = document.getElementById('shortcutsModal');
            if (modal) modal.classList.toggle('open');
            return;
        }

        // / key — focus search (only when not in input)
        if (e.key === '/' && !isInput) {
            e.preventDefault();
            var searchInput = document.querySelector('.search-box input');
            if (searchInput) searchInput.focus();
            return;
        }

        // Ctrl+S / Cmd+S — save article
        if ((e.ctrlKey || e.metaKey) && e.key === 's') {
            e.preventDefault();
            var saveBtn = document.getElementById('saveBtn');
            if (saveBtn && typeof saveArticle === 'function') {
                saveArticle(false);
            } else if (saveBtn) {
                saveBtn.click();
            }
            return;
        }

        // Ctrl+P / Cmd+P — preview
        if ((e.ctrlKey || e.metaKey) && !e.shiftKey && e.key === 'p') {
            e.preventDefault();
            if (typeof openPreview === 'function') {
                openPreview();
            } else {
                // Try opening preview in new tab for article list
                var previewLink = document.querySelector('a[href*="/admin/preview/"]');
                if (previewLink) window.open(previewLink.href, '_blank');
            }
            return;
        }

        // Ctrl+Shift+P — publish (if in ready status)
        if ((e.ctrlKey || e.metaKey) && e.shiftKey && (e.key === 'p' || e.key === 'P')) {
            e.preventDefault();
            var statusHidden = document.getElementById('articleStatusHidden');
            if (statusHidden && typeof getSelectedStatus === 'function') {
                var st = getSelectedStatus();
                if (st === 'ready' || st === 'draft') {
                    // Find publish workflow action button
                    var pubBtn = document.querySelector('[data-wf-action="publish"]');
                    if (pubBtn) pubBtn.click();
                }
            }
            return;
        }
    });


    // ===== SIDEBAR MOBILE =====
    document.addEventListener('click', function(e) {
        if (document.body.classList.contains('sidebar-open') &&
            !e.target.closest('.sidebar') &&
            !e.target.closest('.menu-toggle')) {
            document.body.classList.remove('sidebar-open');
        }
    });

    // Swipe-to-close sidebar on mobile
    (function() {
        var startX = 0, startY = 0, swiping = false;
        var sidebar = document.querySelector('.sidebar');
        if (!sidebar) return;

        sidebar.addEventListener('touchstart', function(e) {
            startX = e.touches[0].clientX;
            startY = e.touches[0].clientY;
            swiping = true;
        }, { passive: true });

        sidebar.addEventListener('touchmove', function(e) {
            if (!swiping) return;
            var dx = e.touches[0].clientX - startX;
            var dy = e.touches[0].clientY - startY;
            if (Math.abs(dx) > Math.abs(dy) && dx < -50) {
                document.body.classList.remove('sidebar-open');
                swiping = false;
            }
        }, { passive: true });

        sidebar.addEventListener('touchend', function() { swiping = false; }, { passive: true });
    })();

    // ===== VIEW TOGGLE (grid / list) =====
    var viewGrid = document.getElementById('viewGrid');
    var viewList = document.getElementById('viewList');
    var toggleBtns = document.querySelectorAll('.view-toggle-btn');

    if (viewGrid && viewList && toggleBtns.length) {
        var savedView = localStorage.getItem('articles-view') || 'grid';
        setView(savedView);

        toggleBtns.forEach(function(btn) {
            btn.addEventListener('click', function() {
                var view = this.dataset.view;
                setView(view);
                localStorage.setItem('articles-view', view);
            });
        });
    }

    function setView(view) {
        if (!viewGrid || !viewList) return;
        toggleBtns.forEach(function(b) { b.classList.toggle('active', b.dataset.view === view); });
        viewGrid.style.display = view === 'grid' ? '' : 'none';
        viewList.style.display = view === 'list' ? '' : 'none';
    }

    // Auto-submit filters on select change
    document.querySelectorAll('.filter-select').forEach(function(sel) {
        sel.addEventListener('change', function() {
            // this.form.submit();
        });
    });
});
