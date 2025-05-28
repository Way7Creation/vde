import "../css/main.css";

import { loadPage, changeItemsPerPage, changePage, handlePageInputKeydown } from './pagination.js';
import { filterByBrandOrSeries, applyFilters, clearAllFilters } from './filters.js';
import { sortProducts } from './sort.js';
import { loadAvailability } from './availability.js';
import { addToCart, clearCart, removeFromCart, fetchCart } from './cart.js';
import { showToast, fetchProducts } from './utils.js';
import { renderProductsTable, copyText } from './renderProducts.js';
import { createSpecification } from './specification.js';
import { smartSearch } from './smartSearch.js';
import { productService } from './services/ProductService.js';

// Инициализация глобальных переменных
window.itemsPerPage = parseInt(sessionStorage.getItem('itemsPerPage') || '20', 10);
window.currentPage = 1;
window.productsData = [];
window.totalProducts = 0;
window.sortColumn = sessionStorage.getItem('sortColumn') || 'name';
window.sortDirection = sessionStorage.getItem('sortDirection') || 'asc';
window.appliedFilters = {};
window.cart = {};

// Восстановление фильтров из sessionStorage
Object.keys(sessionStorage).forEach(key => {
    if (!['itemsPerPage', 'sortColumn', 'sortDirection'].includes(key)) {
        window.appliedFilters[key] = sessionStorage.getItem(key);
    }
});

// Экспорт функций в window для обратной совместимости
window.renderProductsTable = renderProductsTable;
window.copyText = copyText;
window.createSpecification = createSpecification;
window.loadAvailability = loadAvailability;
window.addToCart = addToCart;
window.clearCart = clearCart;
window.removeFromCart = removeFromCart;
window.fetchCart = fetchCart;
window.filterByBrandOrSeries = filterByBrandOrSeries;
window.fetchProducts = fetchProducts;
window.sortProducts = sortProducts;
window.loadPage = loadPage;

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    // Город
    const citySelect = document.getElementById('citySelect');
    if (citySelect) {
        citySelect.value = localStorage.getItem('selectedCityId') || '1';
        citySelect.addEventListener('change', () => {
            localStorage.setItem('selectedCityId', citySelect.value);
            // Очищаем кеш при смене города
            productService.clearCache();
            if (window.productsData.length > 0) {
                fetchProducts();
            }
        });
    }
    
    // Количество товаров на странице
    ['itemsPerPageSelect', 'itemsPerPageSelectBottom'].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.value = window.itemsPerPage;
            el.addEventListener('change', changeItemsPerPage);
        }
    });
    
    // Ввод номера страницы
    ['pageInput', 'pageInputBottom'].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.addEventListener('change', changePage);
            el.addEventListener('keydown', handlePageInputKeydown);
        }
    });
    
    // Выбрать все
    const selectAllEl = document.getElementById('selectAll');
    if (selectAllEl) {
        selectAllEl.addEventListener('change', event => {
            const isChecked = event.target.checked;
            document.querySelectorAll('.product-checkbox').forEach(checkbox => {
                checkbox.checked = isChecked;
            });
        });
    }
    
    // Обработчики кликов
    document.body.addEventListener('click', handleBodyClick);
    
    // Кнопки пагинации
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
    
    // Загрузка товаров если мы на странице каталога
    if (document.querySelector('.product-table')) {
        loadPage(window.currentPage);
    }
    
    // Загрузка корзины
    if (document.querySelector('.cart-container') || document.getElementById('cartBadge')) {
        fetchCart().catch(console.error);
    }
});

// Обработчик кликов по body
function handleBodyClick(e) {
    const target = e.target;
    
    // Добавить в корзину
    if (target.closest('.add-to-cart-btn')) {
        const btn = target.closest('.add-to-cart-btn');
        const productId = btn.dataset.productId;
        const quantityInput = btn.closest('tr')?.querySelector('.quantity-input');
        const quantity = parseInt(quantityInput?.value || '1', 10);
        addToCart(productId, quantity);
        return;
    }
    
    // Удалить из корзины
    if (target.closest('.remove-from-cart-btn')) {
        const btn = target.closest('.remove-from-cart-btn');
        removeFromCart(btn.dataset.productId);
        return;
    }
    
    // Очистить корзину
    if (target.matches('#clearCartBtn') || target.closest('.clear-cart-btn')) {
        if (confirm('Очистить корзину?')) {
            clearCart();
        }
        return;
    }
    
    // Создать спецификацию
    if (target.closest('.create-specification-btn')) {
        e.preventDefault();
        createSpecification();
        return;
    }
    
    // Сортировка
    const sortableHeader = target.closest('th.sortable');
    if (sortableHeader && sortableHeader.dataset.column) {
        sortProducts(sortableHeader.dataset.column);
        return;
    }
}