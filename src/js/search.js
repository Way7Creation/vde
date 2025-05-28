import { showToast } from './utils.js';

/**
 * Универсальный запрос к OpenSearch.
 * @param {Object} options — либо { page, itemsPerPage, sortColumn, sortDirection, filters }
 *                         либо { ids: [1,2,3,...] }
 */
export async function fetchFromOpenSearch(options) {
    let params = {};
    if (Array.isArray(options.ids)) {
        // корзина: передаём только IDs
        params = { ids: options.ids.join(',') };
    } else {
        // каталог
        params = {
            page:           options.page,
            itemsPerPage:   options.itemsPerPage,
            sortColumn:     options.sortColumn,
            sortDirection:  options.sortDirection,
            filters:        JSON.stringify(options.filters || {})
        };
    }

    const url = `/get_protop.php?${new URLSearchParams(params).toString()}`;
    try {
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(resp.statusText);
        const data = await resp.json();
        if (data.error) {
            showToast(data.error, true);
            return { products: [], totalProducts: 0 };
        }
        return { products: data.products, totalProducts: data.totalProducts };
    } catch (err) {
        console.error('OpenSearch error:', err);
        showToast('Ошибка поиска', true);
        return { products: [], totalProducts: 0 };
    }
}