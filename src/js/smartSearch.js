import { showToast } from './utils.js';

class SmartSearch {
    constructor() {
        this.searchInput = document.getElementById('searchInput');
        this.autocompleteContainer = null;
        this.searchTimeout = null;
        this.autocompleteTimeout = null;
        this.selectedIndex = -1;
        this.lastQuery = '';
        this.cache = new Map();
        
        this.init();
    }
    
    init() {
        if (!this.searchInput) return;
        
        this.createAutocompleteContainer();
        this.bindEvents();
    }
    
    createAutocompleteContainer() {
        this.autocompleteContainer = document.createElement('div');
        this.autocompleteContainer.className = 'search-autocomplete';
        this.autocompleteContainer.style.cssText = `
            position: absolute;
            top: 100%;
            left: 0;
            right: 0;
            background: white;
            border: 1px solid #ddd;
            border-top: none;
            border-radius: 0 0 4px 4px;
            max-height: 400px;
            overflow-y: auto;
            display: none;
            z-index: 1000;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        `;
        
        this.searchInput.parentElement.style.position = 'relative';
        this.searchInput.parentElement.appendChild(this.autocompleteContainer);
    }
    
    bindEvents() {
        this.searchInput.addEventListener('input', this.handleInput.bind(this));
        this.searchInput.addEventListener('keydown', this.handleKeydown.bind(this));
        this.searchInput.addEventListener('focus', this.handleFocus.bind(this));
        this.searchInput.addEventListener('blur', () => {
            setTimeout(() => this.hideAutocomplete(), 200);
        });
        
        document.addEventListener('click', (e) => {
            if (!this.searchInput.contains(e.target) && 
                !this.autocompleteContainer.contains(e.target)) {
                this.hideAutocomplete();
            }
        });
    }
    
    handleInput(event) {
        const query = event.target.value.trim();
        
        // Сохраняем в фильтры
        if (query) {
            window.appliedFilters.search = query;
            sessionStorage.setItem('search', query);
        } else {
            delete window.appliedFilters.search;
            sessionStorage.removeItem('search');
        }
        
        // Автодополнение
        clearTimeout(this.autocompleteTimeout);
        if (query.length >= 2) {
            this.autocompleteTimeout = setTimeout(() => {
                this.fetchAutocomplete(query);
            }, 150);
        } else {
            this.hideAutocomplete();
        }
        
        // Основной поиск с дебаунсом
        clearTimeout(this.searchTimeout);
        this.searchTimeout = setTimeout(() => {
            this.performSearch();
        }, 300);
    }
    
    handleKeydown(event) {
        const items = this.autocompleteContainer.querySelectorAll('.autocomplete-item');
        
        switch(event.key) {
            case 'ArrowDown':
                event.preventDefault();
                this.selectedIndex = Math.min(this.selectedIndex + 1, items.length - 1);
                this.highlightItem(items);
                break;
                
            case 'ArrowUp':
                event.preventDefault();
                this.selectedIndex = Math.max(this.selectedIndex - 1, -1);
                this.highlightItem(items);
                break;
                
            case 'Enter':
                event.preventDefault();
                if (this.selectedIndex >= 0 && items[this.selectedIndex]) {
                    this.selectAutocompleteItem(items[this.selectedIndex]);
                } else {
                    this.performSearch();
                }
                break;
                
            case 'Escape':
                this.hideAutocomplete();
                break;
        }
    }
    
    handleFocus() {
        if (this.searchInput.value.length >= 2) {
            this.fetchAutocomplete(this.searchInput.value);
        }
    }
    
    highlightItem(items) {
        items.forEach((item, index) => {
            if (index === this.selectedIndex) {
                item.classList.add('highlighted');
                item.scrollIntoView({ block: 'nearest' });
            } else {
                item.classList.remove('highlighted');
            }
        });
    }
    
    async fetchAutocomplete(query) {
        // Проверяем кеш
        if (this.cache.has(query)) {
            this.showAutocomplete(this.cache.get(query));
            return;
        }
        
        try {
            const response = await fetch(`/search_autocomplete.php?q=${encodeURIComponent(query)}`);
            const data = await response.json();
            
            // Кешируем результат
            this.cache.set(query, data.suggestions);
            
            // Очищаем старый кеш
            if (this.cache.size > 50) {
                const firstKey = this.cache.keys().next().value;
                this.cache.delete(firstKey);
            }
            
            this.showAutocomplete(data.suggestions);
        } catch (error) {
            console.error('Ошибка автодополнения:', error);
        }
    }
    
    showAutocomplete(suggestions) {
        if (!suggestions || suggestions.length === 0) {
            this.hideAutocomplete();
            return;
        }
        
        this.autocompleteContainer.innerHTML = '';
        this.selectedIndex = -1;
        
        // Группируем предложения по типу
        const grouped = this.groupSuggestions(suggestions);
        
        // Отображаем группы
        Object.entries(grouped).forEach(([type, items]) => {
            if (items.length === 0) return;
            
            // Заголовок группы
            const header = document.createElement('div');
            header.className = 'autocomplete-header';
            header.style.cssText = `
                padding: 8px 12px;
                font-size: 12px;
                color: #666;
                background: #f5f5f5;
                font-weight: 600;
                border-bottom: 1px solid #eee;
            `;
            header.textContent = this.getGroupTitle(type);
            this.autocompleteContainer.appendChild(header);
            
            // Элементы группы
            items.forEach(suggestion => {
                const item = document.createElement('div');
                item.className = 'autocomplete-item';
                item.style.cssText = `
                    padding: 10px 12px;
                    cursor: pointer;
                    border-bottom: 1px solid #eee;
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    transition: background-color 0.2s;
                `;
                item.dataset.text = suggestion.text;
                
                // Текст с подсветкой
                const text = document.createElement('span');
                text.innerHTML = this.highlightQuery(suggestion.text, this.searchInput.value);
                item.appendChild(text);
                
                // Иконка типа
                const icon = document.createElement('span');
                icon.style.cssText = 'font-size: 12px; color: #999;';
                icon.textContent = this.getTypeIcon(type);
                item.appendChild(icon);
                
                // Обработчики
                item.addEventListener('mouseenter', () => {
                    this.autocompleteContainer.querySelectorAll('.autocomplete-item').forEach(el => {
                        el.classList.remove('highlighted');
                    });
                    item.classList.add('highlighted');
                });
                
                item.addEventListener('click', () => {
                    this.selectAutocompleteItem(item);
                });
                
                this.autocompleteContainer.appendChild(item);
            });
        });
        
        this.autocompleteContainer.style.display = 'block';
    }
    
    groupSuggestions(suggestions) {
        const grouped = {
            products: [],
            codes: [],
            brands: [],
            categories: []
        };
        
        suggestions.forEach(suggestion => {
            const type = suggestion.type || 'product';
            
            switch(type) {
                case 'code':
                    grouped.codes.push(suggestion);
                    break;
                case 'brand':
                    grouped.brands.push(suggestion);
                    break;
                case 'category':
                    grouped.categories.push(suggestion);
                    break;
                default:
                    grouped.products.push(suggestion);
            }
        });
        
        return grouped;
    }
    
    getGroupTitle(type) {
        const titles = {
            products: 'Товары',
            codes: 'Артикулы',
            brands: 'Бренды',
            categories: 'Категории'
        };
        return titles[type] || type;
    }
    
    getTypeIcon(type) {
        const icons = {
            products: '📦',
            codes: '🔢',
            brands: '🏷️',
            categories: '📁'
        };
        return icons[type] || '';
    }
    
    highlightQuery(text, query) {
        const regex = new RegExp(`(${this.escapeRegex(query)})`, 'gi');
        return text.replace(regex, '<strong>$1</strong>');
    }
    
    escapeRegex(str) {
        return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }
        
    selectAutocompleteItem(item) {
        const text = item.dataset.text;
        this.searchInput.value = text;
        window.appliedFilters.search = text;
        sessionStorage.setItem('search', text);
        this.hideAutocomplete();
        this.performSearch();
    }
    
    hideAutocomplete() {
        this.autocompleteContainer.style.display = 'none';
        this.selectedIndex = -1;
    }
    
    async performSearch() {
        const query = this.searchInput.value.trim();
        
        if (query === this.lastQuery) return;
        this.lastQuery = query;
        
        // Показываем индикатор загрузки
        this.showSearchIndicator();
        
        try {
            await window.fetchProducts();
            
            // Анализируем результаты
            if (window.productsData.length === 0 && query) {
                this.suggestAlternatives(query);
            } else {
                // Убираем предыдущие предложения
                const oldSuggestions = document.querySelector('.search-suggestions');
                if (oldSuggestions) oldSuggestions.remove();
            }
        } catch (error) {
            showToast('Ошибка поиска', true);
        } finally {
            this.hideSearchIndicator();
        }
    }
    
    showSearchIndicator() {
        const existing = this.searchInput.parentElement.querySelector('.search-indicator');
        if (existing) existing.remove();
        
        const indicator = document.createElement('div');
        indicator.className = 'search-indicator';
        indicator.innerHTML = '<div class="spinner-border spinner-border-sm"></div> Поиск...';
        indicator.style.cssText = `
            position: absolute;
            top: 50%;
            right: 10px;
            transform: translateY(-50%);
            font-size: 12px;
            color: #666;
        `;
        this.searchInput.parentElement.appendChild(indicator);
    }
    
    hideSearchIndicator() {
        const indicator = this.searchInput.parentElement.querySelector('.search-indicator');
        if (indicator) indicator.remove();
    }
    
    async suggestAlternatives(query) {
        const container = document.createElement('div');
        container.className = 'search-suggestions';
        container.style.cssText = `
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 4px;
            padding: 15px;
            margin: 20px 0;
        `;
        
        container.innerHTML = `
            <h5>Ничего не найдено по запросу "${query}"</h5>
            <p>Попробуйте:</p>
            <ul>
                <li>Проверить правильность написания</li>
                <li>Использовать более общие термины</li>
                <li>Искать по коду товара или бренду</li>
            </ul>
        `;
        
        // Предлагаем похожие запросы
        const alternatives = this.findAlternatives(query);
        if (alternatives.length > 0) {
            const altDiv = document.createElement('div');
            altDiv.innerHTML = '<p><strong>Возможно, вы искали:</strong></p>';
            
            alternatives.forEach(alt => {
                const link = document.createElement('a');
                link.href = '#';
                link.textContent = alt;
                link.style.cssText = 'display: inline-block; margin: 0 10px 5px 0; color: #0066cc;';
                link.addEventListener('click', (e) => {
                    e.preventDefault();
                    this.searchInput.value = alt;
                    window.appliedFilters.search = alt;
                    this.performSearch();
                });
                altDiv.appendChild(link);
            });
            
            container.appendChild(altDiv);
        }
        
        const resultsContainer = document.querySelector('.product-table')?.parentElement;
        if (resultsContainer) {
            resultsContainer.insertBefore(container, resultsContainer.firstChild);
        }
    }
    
    findAlternatives(query) {
        const alternatives = [];
        
        // Убираем окончания
        if (query.length > 5) {
            alternatives.push(query.slice(0, -2));
        }
        
        // Разбиваем на слова
        const words = query.split(/\s+/);
        if (words.length > 1) {
            alternatives.push(words[0]);
            alternatives.push(words[words.length - 1]);
        }
        
        // Исправляем раскладку
        const layoutFixed = this.fixKeyboardLayout(query);
        if (layoutFixed !== query) {
            alternatives.push(layoutFixed);
        }
        
        return [...new Set(alternatives)].slice(0, 5);
    }
    
    fixKeyboardLayout(text) {
        const ruToEn = {
            'й':'q', 'ц':'w', 'у':'e', 'к':'r', 'е':'t', 'н':'y', 'г':'u', 'ш':'i', 'щ':'o', 'з':'p',
            'ф':'a', 'ы':'s', 'в':'d', 'а':'f', 'п':'g', 'р':'h', 'о':'j', 'л':'k', 'д':'l',
            'я':'z', 'ч':'x', 'с':'c', 'м':'v', 'и':'b', 'т':'n', 'ь':'m'
        };
        
        const enToRu = Object.fromEntries(Object.entries(ruToEn).map(([k, v]) => [v, k]));
        
        // Проверяем, какая раскладка
        const isRussian = /[а-яА-Я]/.test(text);
        const mapping = isRussian ? ruToEn : enToRu;
        
        return text.split('').map(char => mapping[char.toLowerCase()] || char).join('');
    }
}

// Экспортируем для использования
export const smartSearch = new SmartSearch();