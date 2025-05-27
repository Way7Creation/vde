<?php
namespace App\Services;

use PDO;
use App\Core\Database;
use App\Core\Session; // Добавляем импорт!
use App\Services\CartService;

class AuthService
{
    /**
     * Инициализация сессии при каждом обращении
     */
    private static function ensureSession(): void
    {
        if (session_status() !== PHP_SESSION_ACTIVE) {
            Session::start(); // Используем наш безопасный класс Session
        }
    }

    public static function login(string $login, string $pass): array
    {
        $pdo = Database::getConnection();
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    
        $stmt = $pdo->prepare("SELECT u.user_id, u.username, u.email, u.password_hash, r.name AS role
            FROM users u
            JOIN roles r ON u.role_id = r.role_id
            WHERE (u.username = :login OR u.email = :login) AND u.is_active = 1
            LIMIT 1
        ");
        $stmt->execute(['login' => $login]);
        $user = $stmt->fetch(PDO::FETCH_ASSOC);
    
        if (!$user || !password_verify($pass, $user['password_hash'])) {
            return [false, 'Неверный логин или пароль'];
        }
    
        // Теперь вызываем безопасную инициализацию
        self::ensureSession();
        
        // Session::start() уже сделает session_regenerate_id
        $_SESSION['user_id']  = $user['user_id'];
        $_SESSION['username'] = $user['username'];
        $_SESSION['role']     = $user['role'];
        unset($_SESSION['is_guest']);
    
        \App\Services\CartService::mergeGuestCartWithUser((int)$user['user_id']);
    
        return [true, ''];
    }

    public static function check(): bool
    {
        self::ensureSession();
        return !empty($_SESSION['user_id']);
    }

    public static function user(): array
    {
        self::ensureSession();
        return [
            'id'       => $_SESSION['user_id']  ?? null,
            'username' => $_SESSION['username'] ?? '',
            'role'     => $_SESSION['role']     ?? 'guest',
        ];
    }

    public static function checkRole(string $role): bool
    {
        return self::user()['role'] === $role;
    }
    
    public static function isAdmin(): bool
    {
        return self::check() && self::checkRole('admin');
    }
}
