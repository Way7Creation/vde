<?php
namespace App\Core;

class Cart
{
    const SESSION_KEY = 'cart';

    public static function get(): array
    {
        if (session_status() !== PHP_SESSION_ACTIVE) session_start();
        return $_SESSION[self::SESSION_KEY] ?? [];
    }

    public static function save(array $cart): void
    {
        if (session_status() !== PHP_SESSION_ACTIVE) session_start();
        $_SESSION[self::SESSION_KEY] = $cart;
        session_write_close();
    }

    public static function add(int $productId, int $quantity = 1): void
    {
        $cart = self::get();
        if (isset($cart[$productId])) {
            $cart[$productId]['quantity'] += $quantity;
        } else {
            $cart[$productId] = ['product_id' => $productId, 'quantity' => $quantity];
        }
        self::save($cart);
    }

    public static function remove(int $productId): void
    {
        $cart = self::get();
        unset($cart[$productId]);
        self::save($cart);
    }

    public static function clear(): void
    {
        self::save([]);
    }

    public static function update(int $productId, int $quantity): void
    {
        $cart = self::get();
        if ($quantity > 0) {
            $cart[$productId]['quantity'] = $quantity;
        } else {
            unset($cart[$productId]);
        }
        self::save($cart);
    }
}