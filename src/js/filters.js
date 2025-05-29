import { fetchProducts } from "./utils.js";
import { renderProductsTable } from "./renderProducts.js";

export function filterByBrandOrSeries(key, value) {
  // Если кликнули по уже выбранному значению - снимаем фильтр
  if (window.appliedFilters[key] === value) {
    delete window.appliedFilters[key];
    sessionStorage.removeItem(key);
  } else {
    window.appliedFilters[key] = value;
    sessionStorage.setItem(key, value);
  }
  fetchProducts();
}

export function applyFilters() {
    fetchProducts();
}

export function clearAllFilters() {
    window.appliedFilters = {};
    Object.keys(sessionStorage).forEach(key => {
        if (!['itemsPerPage','sortColumn','sortDirection'].includes(key)) {
            sessionStorage.removeItem(key);
        }
    });
    fetchProducts();
}

export function handleSearchInput(event) {
    const value = event.target.value.trim();
    if (value) {
        window.appliedFilters.search = value;
        sessionStorage.setItem('search', value);
    } else {
        delete window.appliedFilters.search;
        sessionStorage.removeItem('search');
    }
    fetchProducts();
}

export function renderAppliedFilters() {
    const container = document.querySelector(".applied-filters");
    if (!container) return;
    container.innerHTML = '';
    Object.entries(window.appliedFilters).forEach(([key, value]) => {
        if (value) {
            const item = document.createElement('span');
            item.className = 'applied-filter';
            item.textContent = `${key}: ${value}`;
            container.appendChild(item);
        }
    });
}

export function highlightFilteredWords() {
    if (!window.appliedFilters.search) return;
    const search = window.appliedFilters.search;
    document.querySelectorAll('.name-cell span').forEach(span => {
        let html = span.textContent.replace(new RegExp(search, 'gi'), (match) => `<mark>${match}</mark>`);
        span.innerHTML = html;
    });
}