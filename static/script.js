document.addEventListener('DOMContentLoaded', function() {
    document.querySelectorAll('select.unrated').forEach(select => {
        let interactedWithMouse = false;
        // For mouse users, detect the click to open the dropdown
        select.addEventListener('mousedown', function() {
            interactedWithMouse = true;
        });
        // For keyboard users, or when the value is actually changed by any means
        select.addEventListener('change', function() {
            this.classList.remove('unrated');
        });
        // When the dropdown is closed, check if it was opened by mouse
        select.addEventListener('blur', function() {
            if (interactedWithMouse) {
                this.classList.remove('unrated');
            }
        });
    });
});