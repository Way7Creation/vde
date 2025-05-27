<?php
namespace App\Services;

use PDO;
use App\Core\Database;
use App\Services\CartService;

class AuthService
{
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
    
        // ОБЯЗАТЕЛЬНО! Стартуем сессию, если не активна
         if (session_status() !== PHP_SESSION_ACTIVE) session_start();
        session_regenerate_id(true);
    
        $_SESSION['user_id']  = $user['user_id'];
        $_SESSION['username'] = $user['username'];
        $_SESSION['role']     = $user['role'];
        unset($_SESSION['is_guest']);
    
        \App\Services\CartService::mergeGuestCartWithUser((int)$user['user_id']);
    
        session_write_close(); // обязательно!
    
        return [true, ''];
    }

    public static function check(): bool
    {
        return !empty($_SESSION['user_id']);
    }

    public static function user(): array
    {
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
        // проверяем, что залогинен и роль равна 'admin'
        return self::check() && self::checkRole('admin');
    }
}