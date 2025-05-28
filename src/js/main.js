// import "../css/global-styles.css";
// import "../css/styles.css";
// import "../css/header.css";
// import "../css/product-card.css";
import "../css/main.css";

import { loadPage, updatePaginationDisplay, changeItemsPerPage, changePage, handlePageInputKeydown } from './pagination.js';
import { filterByBrandOrSeries, applyFilters, clearAllFilters, handleSearchInput, renderAppliedFilters, highlightFilteredWords } from './filters.js';
import { sortProducts } from './sort.js';
import { loadAvailability } from './availability.js';
import { addToCart, clearCart, removeFromCart, fetchCart } from './cart.js';
import { showToast } from './utils.js';
import { renderProductsTable, copyText } from './renderProducts.js';
import { createSpecification } from './specification.js';
import { smartSearch } from './smartSearch.js';
import { productService, stateManager } from './services/ProductService.js';

window.itemsPerPage   = parseInt(sessionStorage.getItem('itemsPerPage')  || '20', 10);
window.currentPage    = 1;
window.productsData   = [];
window.totalProducts  = 5000;
window.sortColumn     = sessionStorage.getItem('sortColumn')    || 'name';
window.sortDirection  = sessionStorage.getItem('sortDirection') || 'asc';
window.appliedFilters = {};
window.cart           = {};
window.searchTimeout  = null;

Object.keys(sessionStorage).forEach(key => {
    if (!['itemsPerPage','sortColumn','sortDirection'].includes(key)) {
        window.appliedFilters[key] = sessionStorage.getItem(key);
    }
});

window.renderProductsTable = renderProductsTable;
window.copyText = copyText;
window.createSpecification = createSpecification;
window.loadAvailability = loadAvailability;
window.addToCart = addToCart;
window.clearCart = clearCart;
window.removeFromCart = removeFromCart;
window.fetchCart = fetchCart;
window.filterByBrandOrSeries = filterByBrandOrSeries;

const citySelect = document.getElementById('citySelect');
if (citySelect) {
    citySelect.value = localStorage.getItem('selectedCityId') || '1';
    citySelect.addEventListener('change', () => {
        localStorage.setItem('selectedCityId', citySelect.value);
        loadAvailability(citySelect.value);
    });
}

// const searchInput = document.getElementById('searchInput');
// if (searchInput) {
//     const saved = window.appliedFilters.search || '';
//     searchInput.value = saved;
//     if (!saved.trim()) delete window.appliedFilters.search;
//     searchInput.addEventListener('input', handleSearchInput);
// }

['itemsPerPageSelect', 'itemsPerPageSelectBottom'].forEach(id => {
    const el = document.getElementById(id);
    if (el) {
        el.value = window.itemsPerPage;
        el.addEventListener('change', changeItemsPerPage);
    }
});

['pageInput', 'pageInputBottom'].forEach(id => {
    const el = document.getElementById(id);
    if (el) {
        el.addEventListener('change', changePage);
        el.addEventListener('keydown', handlePageInputKeydown);
    }
});

const selectAllEl = document.getElementById('selectAll');
if (selectAllEl) {
    selectAllEl.addEventListener('change', event => {
        const isChecked = event.target.checked;
        const checkboxes = document.querySelectorAll('.product-checkbox');
        checkboxes.forEach(checkbox => {
            checkbox.checked = isChecked;
        });
    });
}

document.body.addEventListener('click', e => {
    const target = e.target;
    if (target.closest('.add-to-cart-btn')) {
        const btn = target.closest('.add-to-cart-btn');
        const productId = btn.dataset.productId;
        const quantityInput = btn.closest('tr')?.querySelector('.quantity-input');
        const quantity = parseInt(quantityInput?.value || '1', 10);
        addToCart(productId, quantity);
        return;
    }
    if (target.closest('.remove-from-cart-btn')) {
        removeFromCart(target.closest('.remove-from-cart-btn').dataset.productId);
        return;
    }
    if (target.matches('#clearCartBtn') || target.closest('.clear-cart-btn')) {
        clearCart();
        return;
    }
    if (target.closest('.create-specification-btn')) {
        e.preventDefault();
        createSpecification();
        return;
    }
});

document.querySelectorAll('.prev-btn').forEach(btn => {
    btn.addEventListener('click', evt => {
        evt.preventDefault();
        loadPage(Math.max(1, window.currentPage - 1));
    });
});
document.querySelectorAll('.next-btn').forEach(btn => {
    btn.addEventListener('click', evt => {
        evt.preventDefault();
        const total = Math.ceil(window.totalProducts / window.itemsPerPage);
        loadPage(Math.min(total, window.currentPage + 1));
    });
});

if (document.querySelector('.product-table')) {
    loadPage(window.currentPage);
}

if (document.querySelector('.cart-container')) {
    fetchCart().catch(console.error);
}
