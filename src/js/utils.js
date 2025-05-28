export function showToast(message, isError = false) {
    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.textContent = message;
    if (isError) toast.classList.add('error');
    document.body.appendChild(toast);
    setTimeout(() => toast.remove(), 2500);
}

// Обновленная функция для работы с новым поиском
export async function fetchFromOpenSearch(params) {
    if (typeof params.filters === 'object') {
        params.filters = JSON.stringify(params.filters);
    }
    const query = new URLSearchParams(params).toString();
    // Используем новый API endpoint
    const res = await fetch(`/api/search_v4.php?${query}`);
    if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
    }
    return await res.json();
}

export async function fetchProducts() {
    showLoadingIndicator();
    try {
        const { products, totalProducts } = await fetchFromOpenSearch({
            page:          window.currentPage,
            itemsPerPage:  window.itemsPerPage,
            sortColumn:    window.sortColumn,
            sortDirection: window.sortDirection,
            filters:       window.appliedFilters
        });
        window.productsData  = products;
        window.totalProducts = totalProducts;
        if (products.length === 0) {
            showToast('Ничего не найдено', false);
        }
        window.renderProductsTable();
        const ids = window.productsData.map(p => p.product_id);
        window.loadAvailability(ids);
    } catch (err) {
        showToast('Ошибка при загрузке продуктов', true);
    } finally {
        hideLoadingIndicator();
    }
}

export function showLoadingIndicator() {
    const loadingIndicator = document.createElement('div');
    loadingIndicator.className = 'loading-indicator';
    loadingIndicator.textContent = 'Загрузка...';
    document.body.appendChild(loadingIndicator);
}

export function hideLoadingIndicator() {
    const loadingIndicator = document.querySelector('.loading-indicator');
    if (loadingIndicator) {
        loadingIndicator.remove();
    }
}
