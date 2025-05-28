import { showToast } from "./utils.js";

export async function fetchCart() {
    try {
        const res = await fetch("/cart/json");
        if (!res.ok) throw new Error("Ошибка загрузки корзины");
        const data = await res.json();
        window.cart = data.cart || {};
    } catch (err) {
        showToast("Ошибка при загрузке корзины", true);
    }
}

export async function addToCart(productId, quantity) {
    try {
        const formData = new FormData();
        formData.append('product_id', productId);
        formData.append('quantity', quantity);
        formData.append('csrf_token', window.CSRF_TOKEN || '');
        
        const res = await fetch("/cart/add", {
            method: "POST",
            body: formData
        });
        
        if (!res.ok) throw new Error("Ошибка добавления в корзину");
        
        const data = await res.json();
        if (data.success) {
            showToast("Товар добавлен в корзину");
            await fetchCart();
        } else {
            showToast(data.message || "Ошибка при добавлении в корзину", true);
        }
    } catch (err) {
        showToast("Ошибка при добавлении в корзину", true);
    }
}

export async function removeFromCart(productId) {
    try {
        const formData = new FormData();
        formData.append('productId', productId);
        formData.append('csrf_token', window.CSRF_TOKEN || '');
        
        const res = await fetch("/cart/remove", {
            method: "POST",
            body: formData
        });
        
        if (!res.ok) throw new Error("Ошибка удаления из корзины");
        
        const data = await res.json();
        if (data.success) {
            showToast("Товар удален из корзины");
            await fetchCart();
        } else {
            showToast(data.message || "Ошибка при удалении из корзины", true);
        }
    } catch (err) {
        showToast("Ошибка при удалении из корзины", true);
    }
}

export async function clearCart() {
    try {
        const formData = new FormData();
        formData.append('csrf_token', window.CSRF_TOKEN || '');
        
        const res = await fetch("/cart/clear", {
            method: "POST",
            body: formData
        });
        
        if (!res.ok) throw new Error("Ошибка очистки корзины");
        
        const data = await res.json();
        if (data.success) {
            showToast("Корзина очищена");
            await fetchCart();
        } else {
            showToast(data.message || "Ошибка при очистке корзины", true);
        }
    } catch (err) {
        showToast("Ошибка при очистке корзины", true);
    }
}