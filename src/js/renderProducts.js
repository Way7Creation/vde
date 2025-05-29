import { filterByBrandOrSeries, renderAppliedFilters, highlightFilteredWords } from "./filters.js";
import { loadAvailability } from "./availability.js";
import { showToast } from "./utils.js";

export function copyText(text) {
    if (!text) {
        showToast('Нечего копировать', true);
        return;
    }
    if (!navigator.clipboard) {
        showToast('Clipboard API не поддерживается', true);
        return;
    }
    navigator.clipboard.writeText(text)
        .then(() => showToast(`Скопировано: ${text}`))
        .catch(() => showToast('Не удалось скопировать', true));
}

export function bindSortableHeaders() {
    const table = document.querySelector('.product-table');
    if (!table) return;
    table.removeEventListener('click', sortableClickHandler);
    table.addEventListener('click', sortableClickHandler);
}

function sortableClickHandler(e) {
    const th = e.target.closest('th.sortable');
    if (th && window.sortProducts) window.sortProducts(th.dataset.column);
}

export function renderProductsTable() {
    const tbody = document.querySelector('.product-table tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    
    window.productsData.forEach(product => {
        const row = document.createElement('tr');
        row.setAttribute('data-product-id', product.product_id);

        // checkbox
        const selectCell = document.createElement('td');
        const selectCheckbox = document.createElement('input');
        selectCheckbox.type = 'checkbox';
        selectCheckbox.classList.add('product-checkbox');
        selectCell.appendChild(selectCheckbox);
        row.appendChild(selectCell);

        // external_id + copy
        const codeCell = document.createElement('td');
        codeCell.classList.add('col-code');
        const codeItem = document.createElement('div');
        codeItem.className = 'item-code';
        const codeSpan = document.createElement('span');
        codeSpan.textContent = product.external_id || '';
        codeItem.appendChild(codeSpan);
        const copyCodeIcon = document.createElement('a');
        copyCodeIcon.className = 'copy-icon js-copy-to-clipboard';
        copyCodeIcon.href = '#';
        copyCodeIcon.setAttribute('data-text-to-copy', product.external_id || '');
        copyCodeIcon.innerHTML = '<i class="far fa-clone"></i>';
        copyCodeIcon.addEventListener('click', e => {
            e.preventDefault();
            copyText(copyCodeIcon.getAttribute('data-text-to-copy'));
        });
        codeItem.appendChild(copyCodeIcon);
        codeCell.appendChild(codeItem);
        row.appendChild(codeCell);

        // image
        const imageCell = document.createElement('td');
        let urls = [];
        if (product.images && Array.isArray(product.images)) {
            urls = product.images;
        } else if (typeof product.image_urls === 'string' && product.image_urls.trim()) {
            urls = product.image_urls.split(',').map(u => u.trim());
        }
        const firstUrl = urls[0] || '/images/placeholder.jpg';
        const container = document.createElement('div');
        container.className = 'image-container';
        container.style.position = 'relative';
        const thumb = document.createElement('img');
        thumb.src = firstUrl;
        thumb.alt = product.name || '';
        thumb.style.width = '50px';
        thumb.style.cursor = 'pointer';
        thumb.style.transition = 'opacity 0.3s ease';
        const zoom = document.createElement('img');
        zoom.className = 'zoom-image';
        zoom.src = firstUrl;
        zoom.alt = product.name || '';
        zoom.style.width = '350px';
        zoom.style.position = 'absolute';
        zoom.style.top = '0';
        zoom.style.left = '60px';
        zoom.style.opacity = '0';
        zoom.style.transition = 'opacity 0.3s ease';
        zoom.style.pointerEvents = 'none';
        zoom.style.zIndex = '1000';
        zoom.style.boxShadow = '0 4px 8px rgba(0,0,0,0.2)';
        zoom.style.backgroundColor = 'white';
        zoom.style.padding = '5px';
        zoom.style.border = '1px solid #ddd';
        zoom.style.borderRadius = '4px';
        
        thumb.addEventListener('mouseenter', () => {
            zoom.style.opacity = '1';
            zoom.style.pointerEvents = 'auto';
        });
        thumb.addEventListener('mouseleave', () => {
            zoom.style.opacity = '0';
            zoom.style.pointerEvents = 'none';
        });
        container.appendChild(thumb);
        container.appendChild(zoom);
        const link = document.createElement('a');
        link.href = `/shop/product?id=${product.external_id}`;
        link.appendChild(container);
        imageCell.appendChild(link);
        row.appendChild(imageCell);

        // name + copy
        const nameCell = document.createElement('td');
        nameCell.className = 'name-cell';
        const nameLink = document.createElement('a');
        nameLink.href = `/shop/product?id=${product.external_id}`;
        nameLink.style.color = 'inherit';
        nameLink.style.textDecoration = 'none';
        const nameItem = document.createElement('div');
        nameItem.className = 'item-code';
        const nameSpan = document.createElement('span');
        
        // Используем подсветку если есть
        if (product._highlight && product._highlight.name) {
            nameSpan.innerHTML = product._highlight.name[0];
        } else {
            nameSpan.textContent = product.name || '';
        }
        
        nameItem.appendChild(nameSpan);
        const copyNameIcon = document.createElement('a');
        copyNameIcon.className = 'copy-icon js-copy-to-clipboard';
        copyNameIcon.href = '#';
        copyNameIcon.setAttribute('data-text-to-copy', product.name || '');
        copyNameIcon.innerHTML = '<i class="far fa-clone"></i>';
        copyNameIcon.addEventListener('click', e => {
            e.preventDefault();
            copyText(copyNameIcon.getAttribute('data-text-to-copy'));
        });
        nameItem.appendChild(copyNameIcon);
        nameLink.appendChild(nameItem);
        nameCell.appendChild(nameLink);
        row.appendChild(nameCell);

        // sku + copy
        const skuCell = document.createElement('td');
        const skuItem = document.createElement('div');
        skuItem.className = 'item-code';
        const skuSpan = document.createElement('span');
        skuSpan.textContent = product.sku || '';
        skuItem.appendChild(skuSpan);
        const copySkuIcon = document.createElement('a');
        copySkuIcon.className = 'copy-icon js-copy-to-clipboard';
        copySkuIcon.href = '#';
        copySkuIcon.setAttribute('data-text-to-copy', product.sku || '');
        copySkuIcon.innerHTML = '<i class="far fa-clone"></i>';
        copySkuIcon.addEventListener('click', e => {
            e.preventDefault();
            copyText(copySkuIcon.getAttribute('data-text-to-copy'));
        });
        skuItem.appendChild(copySkuIcon);
        skuCell.appendChild(skuItem);
        row.appendChild(skuCell);

        // brand / series
        const brandSeriesCell = document.createElement('td');
        const brandSeriesDiv = document.createElement('div');
        const brandSpan = document.createElement('span');
        brandSpan.className = 'brand-name';
        brandSpan.textContent = product.brand_name || '';
        brandSpan.style.cursor = 'pointer';
        brandSpan.addEventListener('click', () => filterByBrandOrSeries('brand_name', product.brand_name));
        const seriesSpan = document.createElement('span');
        seriesSpan.className = 'series-name';
        seriesSpan.textContent = product.series_name || '';
        seriesSpan.style.cursor = 'pointer';
        seriesSpan.addEventListener('click', () => filterByBrandOrSeries('series_name', product.series_name));
        if (brandSpan.textContent && seriesSpan.textContent) {
            brandSpan.textContent += ' / ';
        }
        brandSeriesDiv.appendChild(brandSpan);
        brandSeriesDiv.appendChild(seriesSpan);
        brandSeriesCell.appendChild(brandSeriesDiv);
        row.appendChild(brandSeriesCell);

        // status
        const statusCell = document.createElement('td');
        const statusSpan = document.createElement('span');
        statusSpan.textContent = product.status || 'Активен';
        statusCell.appendChild(statusSpan);
        row.appendChild(statusCell);

        // min_sale_unit
        const minSaleUnitCell = document.createElement('td');
        const minSaleSpan = document.createElement('span');
        minSaleSpan.textContent = product.min_sale || '';
        const unitSpan = document.createElement('span');
        unitSpan.textContent = product.unit ? ` / ${product.unit}` : '';
        minSaleUnitCell.appendChild(minSaleSpan);
        minSaleUnitCell.appendChild(unitSpan);
        row.appendChild(minSaleUnitCell);

        // availability
        const availabilityCell = document.createElement('td');
        availabilityCell.classList.add('col-availability');
        const availabilitySpan = document.createElement('span');
        
        // Если данные о наличии уже есть
        if (product.stock) {
            const qty = product.stock.quantity || 0;
            availabilitySpan.textContent = qty > 0 ? `${qty} шт.` : "Нет";
            availabilitySpan.classList.toggle('in-stock', qty > 0);
            availabilitySpan.classList.toggle('out-of-stock', qty === 0);
        } else {
            availabilitySpan.textContent = '…';
        }
        
        availabilityCell.appendChild(availabilitySpan);
        row.appendChild(availabilityCell);

        // delivery_date
        const deliveryDateCell = document.createElement('td');
        deliveryDateCell.classList.add('col-delivery-date');
        const deliveryDateSpan = document.createElement('span');
        
        // Если данные о доставке уже есть
        if (product.delivery) {
            deliveryDateSpan.textContent = product.delivery.date || product.delivery.text || '—';
        } else {
            deliveryDateSpan.textContent = '…';
        }
        
        deliveryDateCell.appendChild(deliveryDateSpan);
        row.appendChild(deliveryDateCell);

        // price
        const priceCell = document.createElement('td');
        const priceSpan = document.createElement('span');
        
        // Используем новую структуру цен
        if (product.price && product.price.final) {
            priceSpan.textContent = `${product.price.final.toFixed(2)} руб.`;
            if (product.price.has_special) {
                priceSpan.innerHTML = `<span class="price-current">${product.price.final.toFixed(2)} руб.</span>`;
            }
        } else if (product.base_price) {
            // Fallback для старой структуры
            priceSpan.textContent = `${product.base_price.toFixed(2)} руб.`;
        } else {
            priceSpan.textContent = 'Нет цены';
        }
        
        priceCell.appendChild(priceSpan);
        priceCell.setAttribute('data-fulltext', priceSpan.textContent);
        row.appendChild(priceCell);

        // retail_price
        const retailPriceCell = document.createElement('td');
        const retailPriceSpan = document.createElement('span');
        
        if (product.price && product.price.base && product.price.has_special) {
            retailPriceSpan.innerHTML = `<span class="price-old">${product.price.base.toFixed(2)} руб.</span>`;
        } else if (product.retail_price) {
            retailPriceSpan.textContent = `${product.retail_price.toFixed(2)} руб.`;
        } else {
            retailPriceSpan.textContent = '—';
        }
        
        retailPriceCell.appendChild(retailPriceSpan);
        retailPriceCell.setAttribute('data-fulltext', retailPriceSpan.textContent);
        row.appendChild(retailPriceCell);

        // cart
        const cartCell = document.createElement('td');
        const quantityInput = document.createElement('input');
        quantityInput.className = 'form-control quantity-input';
        quantityInput.type = 'number';
        quantityInput.value = 1;
        quantityInput.min = 1;
        const addToCartButton = document.createElement('button');
        addToCartButton.className = 'add-to-cart-btn';
        addToCartButton.innerHTML = '<i class="fas fa-shopping-cart"></i>';
        addToCartButton.dataset.productId = product.product_id;
        cartCell.appendChild(quantityInput);
        cartCell.appendChild(addToCartButton);
        row.appendChild(cartCell);

        // additional_fields
        const additionalFieldsCell = document.createElement('td');
        const additionalFieldsSpan = document.createElement('span');
        additionalFieldsSpan.textContent = 'Доп. информация';
        additionalFieldsCell.appendChild(additionalFieldsSpan);
        row.appendChild(additionalFieldsCell);

        // orders_count
        const ordersCountCell = document.createElement('td');
        const ordersCountSpan = document.createElement('span');
        ordersCountSpan.textContent = product.orders_count || '0';
        ordersCountCell.appendChild(ordersCountSpan);
        row.appendChild(ordersCountCell);

        tbody.appendChild(row);
    });

    // После отрисовки — обновить интерфейс
    if (typeof window.updatePaginationDisplay === "function") window.updatePaginationDisplay();
    if (typeof window.renderAppliedFilters === "function") window.renderAppliedFilters();
    if (typeof window.highlightFilteredWords === "function") window.highlightFilteredWords();

    // Загрузить остатки, если они еще не загружены
    const productsNeedingAvailability = window.productsData.filter(p => !p.stock && !p.delivery);
    if (productsNeedingAvailability.length > 0) {
        const ids = productsNeedingAvailability.map(p => p.product_id);
        loadAvailability(ids);
    }

    // colResizable - проверяем наличие jQuery
    try {
        if (typeof jQuery !== 'undefined' && jQuery.fn.colResizable) {
            const $table = jQuery('#productTable');
            if ($table.length > 0) {
                $table.colResizable('destroy');
                $table.colResizable({
                    liveDrag: true,
                    minWidth: 30,
                    hoverCursor: "col-resize"
                });
            }
        }
    } catch (e) {
        console.warn('colResizable не инициализирован:', e.message);
    }
}