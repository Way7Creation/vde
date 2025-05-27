<?php
namespace App\Services;

use App\Core\Database;

class CartService
{
    const SESSION_KEY = 'cart';

    /**
     * Сохраняет корзину пользователя в БД и сессию.
     */
    public static function saveToDatabase(int $userId, array $cart): void
    {
        if ($userId > 0) {
            $pdo = Database::getConnection();
            $payload = json_encode($cart);
            $stmt = $pdo->prepare("
                INSERT INTO carts (user_id, payload, created_at, updated_at)
                VALUES (:uid, :pl, NOW(), NOW())
                ON DUPLICATE KEY UPDATE payload = VALUES(payload), updated_at = NOW()
            ");
            $stmt->execute([
                'uid' => $userId,
                'pl'  => $payload,
            ]);
        }
        // В сессию обновляем для текущего пользователя
        if (session_status() !== PHP_SESSION_ACTIVE) session_start();
        $_SESSION[self::SESSION_KEY] = $cart;
        session_write_close();
    }

    /**
     * Загружает корзину пользователя из БД или гостя из сессии.
     */
    public static function load(int $userId): array
    {
        if ($userId > 0) {
            $pdo = Database::getConnection();
            $stmt = $pdo->prepare("SELECT payload FROM carts WHERE user_id = ? LIMIT 1");
            $stmt->execute([$userId]);
            $dbPayload = $stmt->fetchColumn();
            $cart = $dbPayload ? json_decode($dbPayload, true) : [];
            // Обновим сессию
            if (session_status() !== PHP_SESSION_ACTIVE) session_start();
            $_SESSION[self::SESSION_KEY] = $cart;
            session_write_close();
            return $cart;
        } else {
            if (session_status() !== PHP_SESSION_ACTIVE) session_start();
            $cart = $_SESSION[self::SESSION_KEY] ?? [];
            session_write_close();
            return $cart;
        }
    }

    /**
     * Очищает корзину (БД и сессия).
     */
    public static function clear(int $userId): void
    {
        if ($userId > 0) {
            $pdo = Database::getConnection();
            $stmt = $pdo->prepare("DELETE FROM carts WHERE user_id = ?");
            $stmt->execute([$userId]);
        }
        if (session_status() !== PHP_SESSION_ACTIVE) session_start();
        $_SESSION[self::SESSION_KEY] = [];
        session_write_close();
    }

    /**
     * Слияние корзины гостя с корзиной пользователя, вызывается после логина.
     */
    public static function mergeGuestCartWithUser(int $userId): void
    {
        if ($userId <= 0) return;
        if (session_status() !== PHP_SESSION_ACTIVE) session_start();
        $guestCart = $_SESSION[self::SESSION_KEY] ?? [];
        // загружаем корзину пользователя из БД
        $userCart = self::load($userId);

        foreach ($guestCart as $pid => $item) {
            if (isset($userCart[$pid])) {
                $userCart[$pid]['quantity'] += $item['quantity'];
            } else {
                $userCart[$pid] = $item;
            }
        }
        self::saveToDatabase($userId, $userCart); // сохраняем всё
        // Обновляем сессию гостя (очищаем)
        $_SESSION[self::SESSION_KEY] = $userCart; // Сессия теперь как у юзера
        session_write_close();
    }
}