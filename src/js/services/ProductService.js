/**
 * Сервис для работы с товарами
 * Использует единый API endpoint
 */
export class ProductService {
    constructor() {
        this.apiUrl = '/api/products.php';
        this.cache = new Map();
        this.cacheTimeout = 5 * 60 * 1000; // 5 минут
    }
    
    /**
     * Поиск товаров
     */
    async search(params = {}) {
        const searchParams = new URLSearchParams({
            action: 'search',
            q: params.query || '',
            page: params.page || 1,
            limit: params.limit || 20,
            sort: params.sort || 'relevance',
            city_id: params.city_id || this.getCurrentCityId()
        });
        
        // Добавляем фильтры
        if (params.filters) {
            searchParams.append('filters', JSON.stringify(params.filters));
        }
        
        const cacheKey = searchParams.toString();
        
        // Проверяем кеш
        const cached = this.getFromCache(cacheKey);
        if (cached) {
            return cached;
        }
        
        try {
            const response = await this.fetchWithTimeout(
                `${this.apiUrl}?${searchParams}`,
                { credentials: 'include' }
            );
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            
            const data = await response.json();
            
            // Сохраняем в кеш только успешные ответы
            if (data.success) {
                this.saveToCache(cacheKey, data);
            }
            
            return data;
            
        } catch (error) {
            console.error('Search error:', error);
            return {
                success: false,
                data: {
                    products: [],
                    total: 0,
                    page: params.page || 1,
                    limit: params.limit || 20,
                    pages: 0
                },
                error: error.message
            };
        }
    }
    
    /**
     * Получить товар по ID
     */
    async getProduct(id, cityId = null) {
        const params = new URLSearchParams({
            action: 'get',
            id: id,
            city_id: cityId || this.getCurrentCityId()
        });
        
        try {
            const response = await this.fetchWithTimeout(
                `${this.apiUrl}?${params}`,
                { credentials: 'include' }
            );
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            
            return await response.json();
            
        } catch (error) {
            console.error('Get product error:', error);
            return {
                success: false,
                error: error.message
            };
        }
    }
    
    /**
     * Автодополнение
     */
    async autocomplete(query) {
        if (!query || query.length < 2) {
            return { success: true, suggestions: [] };
        }
        
        const params = new URLSearchParams({
            action: 'autocomplete',
            q: query
        });
        
        try {
            const response = await this.fetchWithTimeout(
                `${this.apiUrl}?${params}`,
                { credentials: 'include' },
                3000 // 3 секунды таймаут для автодополнения
            );
            
            if (!response.ok) {
                return { success: false, suggestions: [] };
            }
            
            return await response.json();
            
        } catch (error) {
            console.error('Autocomplete error:', error);
            return { success: false, suggestions: [] };
        }
    }
    
    /**
     * Получить текущий ID города
     */
    getCurrentCityId() {
        const citySelect = document.getElementById('citySelect');
        return citySelect ? parseInt(citySelect.value) : 1;
    }
    
    /**
     * Запрос с таймаутом
     */
    async fetchWithTimeout(url, options = {}, timeout = 10000) {
        const controller = new AbortController();
        const id = setTimeout(() => controller.abort(), timeout);
        
        try {
            const response = await fetch(url, {
                ...options,
                signal: controller.signal
            });
            clearTimeout(id);
            return response;
        } catch (error) {
            clearTimeout(id);
            if (error.name === 'AbortError') {
                throw new Error('Превышено время ожидания');
            }
            throw error;
        }
    }
    
    /**
     * Работа с кешем
     */
    getFromCache(key) {
        const cached = this.cache.get(key);
        if (!cached) return null;
        
        if (Date.now() - cached.timestamp > this.cacheTimeout) {
            this.cache.delete(key);
            return null;
        }
        
        return cached.data;
    }
    
    saveToCache(key, data) {
        if (this.cache.size > 50) {
            const firstKey = this.cache.keys().next().value;
            this.cache.delete(firstKey);
        }
        
        this.cache.set(key, {
            timestamp: Date.now(),
            data: data
        });
    }
    
    clearCache() {
        this.cache.clear();
    }
}

// Экспортируем синглтон
export const productService = new ProductService();