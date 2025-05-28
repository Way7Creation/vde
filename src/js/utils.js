import { productService } from './services/ProductService.js';

export function showToast(message, isError = false) {
    const toast = document.createElement('div');
    toast.className = `toast ${isError ? 'toast-error' : 'toast-success'} show`;
    toast.innerHTML = `
        <div class="toast-content">
            <div class="toast-message">${message}</div>
        </div>
    `;
    
    const container = document.getElementById('toastContainer') || document.body;
    container.appendChild(toast);
    
    setTimeout(() => toast.remove(), 3000);
}

export async function fetchProducts() {
    showLoadingIndicator();
    
    try {
        // Собираем параметры
        const params = {
            query: window.appliedFilters.search || '',
            page: window.currentPage || 1,
            limit: window.itemsPerPage || 20,
            sort: getSortParam(),
            filters: { ...window.appliedFilters }
        };
        
        // Удаляем search из filters, так как он передается отдельно
        delete params.filters.search;
        
        // Получаем данные
        const result = await productService.search(params);
        
        if (result.success) {
            window.productsData = result.data.products;
            window.totalProducts = result.data.total;
            
            // Обновляем UI
            window.renderProductsTable();
            updatePaginationInfo();
            
            // Загружаем данные о наличии
            if (window.productsData.length > 0) {
                const ids = window.productsData.map(p => p.product_id);
                window.loadAvailability(ids);
            }
        } else {
            showToast(result.error || 'Ошибка загрузки товаров', true);
            window.productsData = [];
            window.totalProducts = 0;
            window.renderProductsTable();
        }
        
    } catch (error) {
        console.error('Fetch products error:', error);
        showToast('Ошибка при загрузке товаров', true);
    } finally {
        hideLoadingIndicator();
    }
}

function getSortParam() {
    const column = window.sortColumn || 'name';
    const direction = window.sortDirection || 'asc';
    
    // Преобразуем в формат API
    if (column === 'base_price') {
        return direction === 'asc' ? 'price_asc' : 'price_desc';
    }
    
    return column === 'name' ? 'name' : 'relevance';
}

function updatePaginationInfo() {
    const totalPages = Math.ceil(window.totalProducts / window.itemsPerPage);
    
    // Обновляем все элементы пагинации
    document.querySelectorAll('#currentPage, #currentPageBottom').forEach(el => {
        el.textContent = window.currentPage;
    });
    
    document.querySelectorAll('#totalPages, #totalPagesBottom').forEach(el => {
        el.textContent = totalPages;
    });
    
    document.querySelectorAll('#totalProductsText, #totalProductsTextBottom').forEach(el => {
        el.textContent = `Найдено товаров: ${window.totalProducts}`;
    });
}

export function showLoadingIndicator() {
    const existing = document.querySelector('.loading-indicator');
    if (existing) return;
    
    const indicator = document.createElement('div');
    indicator.className = 'loading-indicator';
    indicator.innerHTML = `
        <div class="spinner-border spinner-border-sm"></div>
        <span>Загрузка...</span>
    `;
    document.body.appendChild(indicator);
}

export function hideLoadingIndicator() {
    const indicator = document.querySelector('.loading-indicator');
    if (indicator) {
        indicator.remove();
    }
}