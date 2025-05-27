/**
 * Сервис для работы с товарами
 * Разделение статических и динамических данных
 */
export class ProductService {
    constructor() {
        this.apiUrl = '/api/search_v4.php';
        this.cache = new Map();
        this.cacheTimeout = 5 * 60 * 1000; // 5 минут
    }
    
    /**
     * Поиск товаров с автоматическим определением типа запроса
     */
    async search(params = {}) {
        const defaultParams = {
            q: '',
            page: 1,
            limit: 20,
            sort: 'relevance',
            city_id: this.getCurrentCityId(),
            filters: {}
        };
        
        const searchParams = { ...defaultParams, ...params };
        
        // Формируем ключ кеша
        const cacheKey = this.getCacheKey(searchParams);
        
        // Проверяем кеш
        const cached = this.getFromCache(cacheKey);
        if (cached) {
            return cached;
        }
        
        try {
            // Преобразуем параметры в URL
            const url = new URL(this.apiUrl, window.location.origin);
            Object.keys(searchParams).forEach(key => {
                if (key === 'filters') {
                    url.searchParams.append(key, JSON.stringify(searchParams[key]));
                } else {
                    url.searchParams.append(key, searchParams[key]);
                }
            });
            
            // Выполняем запрос с защитой от таймаута
            const response = await this.fetchWithTimeout(url, {
                method: 'GET',
                credentials: 'include',
                headers: {
                    'Accept': 'application/json',
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            
            if (!data.success) {
                throw new Error(data.error?.message || 'Ошибка поиска');
            }
            
            // Сохраняем в кеш
            this.saveToCache(cacheKey, data);
            
            return data;
            
        } catch (error) {
            console.error('Search error:', error);
            
            // Возвращаем пустой результат при ошибке
            return {
                success: false,
                data: {
                    products: [],
                    total: 0,
                    page: searchParams.page,
                    limit: searchParams.limit,
                    pages: 0
                },
                error: {
                    message: error.message || 'Ошибка поиска'
                }
            };
        }
    }
    
    /**
     * Умное автодополнение
     */
    async autocomplete(query) {
        if (!query || query.length < 2) {
            return [];
        }
        
        try {
            const response = await this.fetchWithTimeout('/api/autocomplete.php?q=' + encodeURIComponent(query), {
                method: 'GET',
                credentials: 'include'
            });
            
            if (!response.ok) {
                return [];
            }
            
            const data = await response.json();
            return data.suggestions || [];
            
        } catch (error) {
            console.error('Autocomplete error:', error);
            return [];
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
                throw new Error('Превышено время ожидания запроса');
            }
            throw error;
        }
    }
    
    /**
     * Получить из кеша
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
    
    /**
     * Сохранить в кеш
     */
    saveToCache(key, data) {
        // Ограничиваем размер кеша
        if (this.cache.size > 50) {
            const firstKey = this.cache.keys().next().value;
            this.cache.delete(firstKey);
        }
        
        this.cache.set(key, {
            timestamp: Date.now(),
            data: data
        });
    }
    
    /**
     * Сформировать ключ кеша
     */
    getCacheKey(params) {
        return JSON.stringify(params);
    }
    
    /**
     * Очистить кеш
     */
    clearCache() {
        this.cache.clear();
    }
}

/**
 * Менеджер состояния приложения
 */
export class StateManager {
    constructor() {
        this.state = {
            products: [],
            total: 0,
            page: 1,
            limit: 20,
            sort: 'relevance',
            filters: {},
            loading: false,
            error: null
        };
        
        this.listeners = new Set();
        this.loadFromStorage();
    }
    
    /**
     * Подписаться на изменения
     */
    subscribe(callback) {
        this.listeners.add(callback);
        return () => this.listeners.delete(callback);
    }
    
    /**
     * Обновить состояние
     */
    setState(updates) {
        this.state = { ...this.state, ...updates };
        this.saveToStorage();
        this.notifyListeners();
    }
    
    /**
     * Получить состояние
     */
    getState() {
        return { ...this.state };
    }
    
    /**
     * Сохранить в localStorage
     */
    saveToStorage() {
        const toSave = {
            page: this.state.page,
            limit: this.state.limit,
            sort: this.state.sort,
            filters: this.state.filters
        };
        
        try {
            localStorage.setItem('productState', JSON.stringify(toSave));
        } catch (error) {
            console.error('Failed to save state:', error);
        }
    }
    
    /**
     * Загрузить из localStorage
     */
    loadFromStorage() {
        try {
            const saved = localStorage.getItem('productState');
            if (saved) {
                const data = JSON.parse(saved);
                this.state = { ...this.state, ...data };
            }
        } catch (error) {
            console.error('Failed to load state:', error);
        }
    }
    
    /**
     * Уведомить подписчиков
     */
    notifyListeners() {
        this.listeners.forEach(callback => {
            try {
                callback(this.state);
            } catch (error) {
                console.error('Listener error:', error);
            }
        });
    }
}

/**
 * Утилиты для работы с товарами
 */
export class ProductUtils {
    /**
     * Форматирование цены
     */
    static formatPrice(price) {
        if (!price || !price.final) {
            return 'Цена по запросу';
        }
        
        const formatted = new Intl.NumberFormat('ru-RU', {
            style: 'currency',
            currency: 'RUB',
            minimumFractionDigits: 2
        }).format(price.final);
        
        if (price.has_special) {
            const oldPrice = new Intl.NumberFormat('ru-RU', {
                style: 'currency',
                currency: 'RUB'
            }).format(price.base);
            
            return `<span class="price-current">${formatted}</span> <span class="price-old">${oldPrice}</span>`;
        }
        
        return `<span class="price-current">${formatted}</span>`;
    }
    
    /**
     * Форматирование наличия
     */
    static formatStock(stock) {
        if (!stock || stock.quantity === 0) {
            return '<span class="text-danger">Нет в наличии</span>';
        }
        
        if (stock.quantity > 10) {
            return '<span class="text-success">В наличии</span>';
        }
        
        return `<span class="text-warning">Осталось ${stock.quantity} шт.</span>`;
    }
    
    /**
     * Форматирование доставки
     */
    static formatDelivery(delivery) {
        if (!delivery) {
            return 'Уточняйте';
        }
        
        const text = delivery.text || 'Уточняйте';
        const date = delivery.date ? ` (${delivery.date})` : '';
        
        return text + date;
    }
    
    /**
     * Подсветка найденного текста
     */
    static highlightText(text, highlights) {
        if (!highlights || highlights.length === 0) {
            return text;
        }
        
        // Берем первую подсветку
        return highlights[0];
    }
    
    /**
     * Определение типа запроса
     */
    static detectQueryType(query) {
        if (!query) return 'empty';
        
        query = query.trim();
        
        // Код товара
        if (/^[A-Za-z0-9\-\.\/\_\s]{2,30}$/u.test(query)) {
            // Проверяем на общие слова
            const commonWords = ['выключатель', 'розетка', 'кабель', 'лампа', 'автомат'];
            const queryLower = query.toLowerCase();
            
            if (commonWords.some(word => queryLower.includes(word))) {
                return 'category';
            }
            
            if (/\d/.test(query) && /[A-Za-z]/.test(query)) {
                return 'code';
            }
        }
        
        // Числовой запрос
        if (/^\d+[\s\-]*(вт|ватт|квт|а|ампер|в|вольт|мм|см|м)?$/i.test(query)) {
            return 'numeric';
        }
        
        // Бренд
        const brands = ['schneider', 'legrand', 'abb', 'iek', 'ekf', 'dkc'];
        if (brands.some(brand => query.toLowerCase().includes(brand))) {
            return 'brand';
        }
        
        return 'text';
    }
}

// Экспортируем синглтоны для удобства
export const productService = new ProductService();
export const stateManager = new StateManager();
