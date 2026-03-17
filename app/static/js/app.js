/* Total.kz Dashboard — minimal JS */
document.addEventListener('DOMContentLoaded', function() {
    // Close sidebar on mobile when clicking outside
    document.addEventListener('click', function(e) {
        if (document.body.classList.contains('sidebar-open') &&
            !e.target.closest('.sidebar') &&
            !e.target.closest('.menu-toggle')) {
            document.body.classList.remove('sidebar-open');
        }
    });

    // Keyboard shortcut: / to focus search
    document.addEventListener('keydown', function(e) {
        if (e.key === '/' && !e.target.matches('input, textarea')) {
            e.preventDefault();
            const searchInput = document.querySelector('.search-box input');
            if (searchInput) searchInput.focus();
        }
    });
});
