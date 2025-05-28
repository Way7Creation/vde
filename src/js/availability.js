import { showToast } from "./utils.js";

export async function loadAvailability(ids) {
    const cityId = document.getElementById('citySelect')?.value || '1';
    
    if (!Array.isArray(ids) || !ids.length) {
        console.warn('loadAvailability: нет товаров для проверки');
        return;
    }
    
    // Разбиваем на батчи по 100 товаров
    const batchSize = 100;
    const batches = [];
    
    for (let i = 0; i < ids.length; i += batchSize) {
        batches.push(ids.slice(i, i + batchSize));
    }
    
    try {
        // Загружаем батчи параллельно
        const promises = batches.map(batch => {
            const params = new URLSearchParams({
                city_id: cityId,
                product_ids: batch.join(',')
            }).toString();
            
            return fetch(`/get_availability.php?${params}`)
                .then(res => {
                    if (!res.ok) throw new Error(`HTTP ${res.status}`);
                    return res.json();
                })
                .catch(err => {
                    console.error('Ошибка загрузки батча:', err);
                    return {}; // Возвращаем пустой объект при ошибке
                });
        });
        
        const results = await Promise.all(promises);
        const data = Object.assign({}, ...results);
        
        // Обновляем UI
        ids.forEach(id => {
            const row = document.querySelector(`tr[data-product-id="${id}"]`);
            if (!row) return;
            
            const availCell = row.querySelector(".availability-cell, .col-availability span");
            const dateCell = row.querySelector(".delivery-date-cell, .col-delivery-date span");
            
            if (data[id]) {
                if (availCell) {
                    const qty = data[id].quantity ?? 0;
                    availCell.textContent = qty > 0 ? `${qty} шт.` : "Нет";
                    availCell.classList.toggle('in-stock', qty > 0);
                    availCell.classList.toggle('out-of-stock', qty === 0);
                }
                
                if (dateCell) {
                    const date = data[id].delivery_date;
                    dateCell.textContent = date || "—";
                }
            } else {
                // Нет данных для этого товара
                if (availCell) availCell.textContent = "—";
                if (dateCell) dateCell.textContent = "—";
            }
        });
        
    } catch (err) {
        console.error('Критическая ошибка при загрузке наличия:', err);
        showToast("Ошибка при загрузке наличия товаров", true);
        
        // Помечаем все ячейки как неопределенные
        ids.forEach(id => {
            const row = document.querySelector(`tr[data-product-id="${id}"]`);
            if (row) {
                const availCell = row.querySelector(".availability-cell, .col-availability span");
                const dateCell = row.querySelector(".delivery-date-cell, .col-delivery-date span");
                if (availCell) availCell.textContent = "Ошибка";
                if (dateCell) dateCell.textContent = "Ошибка";
            }
        });
    }
}