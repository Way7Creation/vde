import { showToast } from './utils.js';
import { productService } from './services/ProductService.js';

class SmartSearch {
    constructor() {
        this.searchInput = document.getElementById('searchInput');
        this.autocompleteContainer = null;
        this.searchTimeout = null;
        this.autocompleteTimeout = null;
        this.selectedIndex = -1;
        this.lastQuery = '';
        
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
    
    async handleInput(event) {
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
            this.autocompleteTimeout = setTimeout(async () => {
                await this.fetchAutocomplete(query);
            }, 150);
        } else {
            this.hideAutocomplete();
        }
        
        // Основной поиск с дебаунсом
        clearTimeout(this.searchTimeout);
        this.searchTimeout = setTimeout(() => {
            window.currentPage = 1; // Сброс на первую страницу
            window.fetchProducts();
        }, 300);
    }
    
    async fetchAutocomplete(query) {
        try {
            const result = await productService.autocomplete(query);
            
            if (result.success) {
                this.showAutocomplete(result.suggestions);
            }
        } catch (error) {
            console.error('Autocomplete error:', error);
        }
    }
    
    showAutocomplete(suggestions) {
        if (!suggestions || suggestions.length === 0) {
            this.hideAutocomplete();
            return;
        }
        
        this.autocompleteContainer.innerHTML = '';
        this.selectedIndex = -1;
        
        suggestions.forEach((suggestion, index) => {
            const item = document.createElement('div');
            item.className = 'autocomplete-item';
            item.dataset.index = index;
            item.dataset.text = suggestion.text;
            item.innerHTML = `
                <span>${this.highlightQuery(suggestion.text, this.searchInput.value)}</span>
                <span class="autocomplete-type">${this.getTypeLabel(suggestion.type)}</span>
            `;
            
            item.addEventListener('click', () => {
                this.selectAutocompleteItem(item);
            });
            
            this.autocompleteContainer.appendChild(item);
        });
        
        this.autocompleteContainer.style.display = 'block';
    }
    
    highlightQuery(text, query) {
        const regex = new RegExp(`(${this.escapeRegex(query)})`, 'gi');
        return text.replace(regex, '<strong>$1</strong>');
    }
    
    escapeRegex(str) {
        return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }
    
    getTypeLabel(type) {
        const labels = {
            code: 'Артикул',
            brand: 'Бренд',
            category: 'Категория',
            text: 'Товар'
        };
        return labels[type] || '';
    }
    
    selectAutocompleteItem(item) {
        const text = item.dataset.text;
        this.searchInput.value = text;
        window.appliedFilters.search = text;
        sessionStorage.setItem('search', text);
        this.hideAutocomplete();
        window.currentPage = 1;
        window.fetchProducts();
    }
    
    hideAutocomplete() {
        this.autocompleteContainer.style.display = 'none';
        this.selectedIndex = -1;
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
                    window.currentPage = 1;
                    window.fetchProducts();
                }
                break;
                
            case 'Escape':
                this.hideAutocomplete();
                break;
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
    
    handleFocus() {
        if (this.searchInput.value.length >= 2) {
            this.fetchAutocomplete(this.searchInput.value);
        }
    }
}

// Экспортируем экземпляр
export const smartSearch = new SmartSearch();