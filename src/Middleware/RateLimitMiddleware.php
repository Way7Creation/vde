<?php
namespace App\Middleware;

use App\Core\Database;
use App\Core\Config;
use App\Core\Logger;

/**
 * Middleware для ограничения частоты запросов (rate limiting)
 */
class RateLimitMiddleware
{
    private static array $cache = [];

    /**
     * Проверить лимит запросов
     */
    public static function handle(?string $identifier = null, int $limit = null, int $window = null): bool
    {
        if (!Config::get('app.security.rate_limit_enabled', true)) {
            return true; // Rate limiting отключен
        }

        $identifier = $identifier ?? self::getIdentifier();
        $limit = $limit ?? Config::get('app.security.rate_limit_requests', 60);
        $window = $window ?? Config::get('app.security.rate_limit_window', 60);

        $endpoint = self::getEndpoint();
        $key = self::generateKey($identifier, $endpoint);

        // Проверяем лимит
        $currentCount = self::getCurrentCount($key, $window);
        
        if ($currentCount >= $limit) {
            Logger::security("Rate limit exceeded", [
                'identifier' => $identifier,
                'endpoint' => $endpoint,
                'current_count' => $currentCount,
                'limit' => $limit,
                'window' => $window
            ]);

            self::sendRateLimitResponse($limit, $window);
            return false;
        }

        // Увеличиваем счетчик
        self::incrementCounter($key, $window);
        
        return true;
    }

    /**
     * Получить идентификатор клиента
     */
    private static function getIdentifier(): string
    {
        // Для аутентифицированных пользователей используем user_id
        if (isset($_SESSION['user_id'])) {
            return 'user_' . $_SESSION['user_id'];
        }

        // Для анонимных - IP адрес
        return 'ip_' . ($_SERVER['REMOTE_ADDR'] ?? '0.0.0.0');
    }

    /**
     * Получить эндпоинт запроса
     */
    private static function getEndpoint(): string
    {
        $uri = $_SERVER['REQUEST_URI'] ?? '/';
        $method = $_SERVER['REQUEST_METHOD'] ?? 'GET';
        
        // Убираем query parameters для группировки
        $path = parse_url($uri, PHP_URL_PATH);
        
        return $method . ':' . $path;
    }

    /**
     * Сгенерировать ключ для кеша
     */
    private static function generateKey(string $identifier, string $endpoint): string
    {
        return "rate_limit:{$identifier}:{$endpoint}";
    }

    /**
     * Получить текущее количество запросов
     */
    private static function getCurrentCount(string $key, int $window): int
    {
        try {
            $windowStart = time() - $window;
            
            $stmt = Database::query(
                "SELECT requests_count FROM rate_limits 
                 WHERE identifier = ? AND endpoint = ? AND window_start > FROM_UNIXTIME(?)
                 ORDER BY window_start DESC LIMIT 1",
                [
                    substr($key, strpos($key, ':') + 1, strpos($key, ':', strpos($key, ':') + 1) - strpos($key, ':') - 1),
                    substr($key, strrpos($key, ':') + 1),
                    $windowStart
                ]
            );

            $result = $stmt->fetch();
            return $result ? (int)$result['requests_count'] : 0;

        } catch (\Exception $e) {
            Logger::error("Rate limit check failed", [
                'key' => $key,
                'error' => $e->getMessage()
            ]);
            return 0; // В случае ошибки разрешаем запрос
        }
    }

    /**
     * Увеличить счетчик запросов
     */
    private static function incrementCounter(string $key, int $window): void
    {
        try {
            $parts = explode(':', $key);
            $identifier = $parts[1];
            $endpoint = $parts[2];
            $windowStart = time();

            Database::query(
                "INSERT INTO rate_limits (identifier, endpoint, requests_count, window_start) 
                 VALUES (?, ?, 1, FROM_UNIXTIME(?))
                 ON DUPLICATE KEY UPDATE 
                 requests_count = requests_count + 1",
                [$identifier, $endpoint, $windowStart]
            );

        } catch (\Exception $e) {
            Logger::error("Rate limit increment failed", [
                'key' => $key,
                'error' => $e->getMessage()
            ]);
        }
    }

    /**
     * Отправить ответ о превышении лимита
     */
    private static function sendRateLimitResponse(int $limit, int $window): void
    {
        http_response_code(429);
        header('Content-Type: application/json');
        header("Retry-After: {$window}");
        header("X-RateLimit-Limit: {$limit}");
        header("X-RateLimit-Window: {$window}");

        echo json_encode([
            'success' => false,
            'error' => 'Too many requests',
            'message' => "Rate limit exceeded. Maximum {$limit} requests per {$window} seconds.",
            'retry_after' => $window
        ]);
        exit;
    }

    /**
     * Очистить старые записи rate limiting
     */
    public static function cleanup(): void
    {
        try {
            $cutoff = time() - 3600; // Удаляем записи старше часа
            
            Database::query(
                "DELETE FROM rate_limits WHERE window_start < FROM_UNIXTIME(?)",
                [$cutoff]
            );

        } catch (\Exception $e) {
            Logger::error("Rate limit cleanup failed", [
                'error' => $e->getMessage()
            ]);
        }
    }
}