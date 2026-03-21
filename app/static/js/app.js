/* Total.kz Dashboard – v3.0 */
document.addEventListener('DOMContentLoaded', function() {

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
        const isDark = theme === 'dark';
        Chart.defaults.color = isDark ? '#9ca3af' : '#6b7280';
        Chart.defaults.borderColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.04)';

        // Update all existing charts
        Chart.helpers.each(Chart.instances, function(chart) {
            if (!chart) return;
            const opts = chart.options;

            // Update tooltip colors
            if (opts.plugins && opts.plugins.tooltip) {
                opts.plugins.tooltip.backgroundColor = isDark ? '#2a2d36' : '#1a1d26';
                opts.plugins.tooltip.titleColor = isDark ? '#e8eaed' : '#ffffff';
                opts.plugins.tooltip.bodyColor = isDark ? '#e8eaed' : '#ffffff';
            }

            // Update scale colors
            if (opts.scales) {
                Object.keys(opts.scales).forEach(function(key) {
                    var scale = opts.scales[key];
                    if (scale.grid) {
                        scale.grid.color = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.04)';
                    }
                    if (scale.ticks) {
                        scale.ticks.color = isDark ? '#9ca3af' : '#6b7280';
                    }
                });
            }

            chart.update('none');
        });
    };

    // Apply chart colors on load
    var savedTheme = localStorage.getItem('theme') || 'light';
    if (savedTheme === 'dark') {
        Chart.defaults.color = '#9ca3af';
        Chart.defaults.borderColor = 'rgba(255,255,255,0.06)';
    }

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

    // ===== KEYBOARD SHORTCUT =====
    document.addEventListener('keydown', function(e) {
        if (e.key === '/' && !e.target.matches('input, textarea, select')) {
            e.preventDefault();
            var searchInput = document.querySelector('.search-box input');
            if (searchInput) searchInput.focus();
        }
    });

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
