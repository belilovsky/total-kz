/* Total.kz Dashboard — v2.0 */
document.addEventListener('DOMContentLoaded', function() {

    // Close sidebar on mobile when clicking overlay
    document.addEventListener('click', function(e) {
        if (document.body.classList.contains('sidebar-open') &&
            !e.target.closest('.sidebar') &&
            !e.target.closest('.menu-toggle')) {
            document.body.classList.remove('sidebar-open');
        }
    });

    // Keyboard shortcut: / to focus search
    document.addEventListener('keydown', function(e) {
        if (e.key === '/' && !e.target.matches('input, textarea, select')) {
            e.preventDefault();
            const searchInput = document.querySelector('.search-box input');
            if (searchInput) searchInput.focus();
        }
    });

    // View toggle (grid / list)
    const viewGrid = document.getElementById('viewGrid');
    const viewList = document.getElementById('viewList');
    const toggleBtns = document.querySelectorAll('.view-toggle-btn');

    if (viewGrid && viewList && toggleBtns.length) {
        // Restore from localStorage
        const savedView = localStorage.getItem('articles-view') || 'grid';
        setView(savedView);

        toggleBtns.forEach(btn => {
            btn.addEventListener('click', function() {
                const view = this.dataset.view;
                setView(view);
                localStorage.setItem('articles-view', view);
            });
        });
    }

    function setView(view) {
        if (!viewGrid || !viewList) return;
        toggleBtns.forEach(b => b.classList.toggle('active', b.dataset.view === view));
        viewGrid.style.display = view === 'grid' ? '' : 'none';
        viewList.style.display = view === 'list' ? '' : 'none';
    }

    // Auto-submit filters on select change (optional UX enhancement)
    document.querySelectorAll('.filter-select').forEach(sel => {
        sel.addEventListener('change', function() {
            // Short delay to allow user to chain filters
            // this.form.submit();
        });
    });
});
