<?php
namespace App\Core;

/**
 * Простой кеш на основе APCu или файлов
 */
class Cache
{
    private static bool $useAPCu = false;
    private static string $cacheDir = '/tmp/cache/';
    
    public static function init(): void
    {
        self::$useAPCu = function_exists('apcu_enabled') && apcu_enabled();
        if (!self::$useAPCu && !is_dir(self::$cacheDir)) {
            mkdir(self::$cacheDir, 0777, true);
        }
    }
    
    public static function get(string $key)
    {
        if (self::$useAPCu) {
            $value = apcu_fetch($key, $success);
            return $success ? $value : null;
        }
        
        $file = self::$cacheDir . md5($key) . '.cache';
        if (!file_exists($file)) {
            return null;
        }
        
        $data = unserialize(file_get_contents($file));
        if ($data['expires'] < time()) {
            unlink($file);
            return null;
        }
        
        return $data['value'];
    }
    
    public static function set(string $key, $value, int $ttl = 3600): bool
    {
        if (self::$useAPCu) {
            return apcu_store($key, $value, $ttl);
        }
        
        $file = self::$cacheDir . md5($key) . '.cache';
        $data = [
            'expires' => time() + $ttl,
            'value' => $value
        ];
        
        return file_put_contents($file, serialize($data), LOCK_EX) !== false;
    }
    
    public static function delete(string $key): bool
    {
        if (self::$useAPCu) {
            return apcu_delete($key);
        }
        
        $file = self::$cacheDir . md5($key) . '.cache';
        if (file_exists($file)) {
            return unlink($file);
        }
        
        return true;
    }
}

/**
 * Rate Limiter для защиты от DDoS
 */
class RateLimiter
{
    private PDO $pdo;
    
    public function __construct()
    {
        $this->pdo = Database::getConnection();
        $this->createTable();
    }
    
    private function createTable(): void
    {
        $sql = "
            CREATE TABLE IF NOT EXISTS rate_limits (
                id VARCHAR(255) PRIMARY KEY,
                action VARCHAR(50) NOT NULL,
                count INT NOT NULL DEFAULT 0,
                window_start INT NOT NULL,
                INDEX idx_window (window_start)
            ) ENGINE=MEMORY
        ";
        
        $this->pdo->exec($sql);
    }
    
    public function check(string $identifier, string $action, int $windowSeconds, int $maxAttempts): bool
    {
        $now = time();
        $windowStart = floor($now / $windowSeconds) * $windowSeconds;
        $key = md5($identifier . ':' . $action . ':' . $windowStart);
        
        // Очистка старых записей
        $this->pdo->exec("DELETE FROM rate_limits WHERE window_start < " . ($now - 3600));
        
        // Проверка текущего лимита
        $stmt = $this->pdo->prepare("
            INSERT INTO rate_limits (id, action, count, window_start) 
            VALUES (:id, :action, 1, :window)
            ON DUPLICATE KEY UPDATE count = count + 1
        ");
        
        $stmt->execute([
            'id' => $key,
            'action' => $action,
            'window' => $windowStart
        ]);
        
        // Получаем текущий счетчик
        $stmt = $this->pdo->prepare("SELECT count FROM rate_limits WHERE id = :id");
        $stmt->execute(['id' => $key]);
        $count = (int)$stmt->fetchColumn();
        
        return $count <= $maxAttempts;
    }
}

/**
 * Валидатор входных данных
 */
class Validator
{
    /**
     * Санитизация строки
     */
    public function sanitizeString(string $input): string
    {
        // Удаляем управляющие символы
        $input = preg_replace('/[\x00-\x1F\x7F]/u', '', $input);
        
        // Обрезаем пробелы
        $input = trim($input);
        
        // Экранируем HTML
        $input = htmlspecialchars($input, ENT_QUOTES | ENT_HTML5, 'UTF-8');
        
        return $input;
    }
    
    /**
     * Валидация целого числа
     */
    public function validateInt($input, int $min = PHP_INT_MIN, int $max = PHP_INT_MAX): int
    {
        $options = [
            'options' => [
                'min_range' => $min,
                'max_range' => $max
            ]
        ];
        
        $result = filter_var($input, FILTER_VALIDATE_INT, $options);
        
        if ($result === false) {
            throw new \InvalidArgumentException('Некорректное целое число');
        }
        
        return $result;
    }
    
    /**
     * Валидация числа с плавающей точкой
     */
    public function validateFloat($input, float $min = -PHP_FLOAT_MAX, float $max = PHP_FLOAT_MAX): float
    {
        $result = filter_var($input, FILTER_VALIDATE_FLOAT);
        
        if ($result === false || $result < $min || $result > $max) {
            throw new \InvalidArgumentException('Некорректное число');
        }
        
        return $result;
    }
    
    /**
     * Валидация email
     */
    public function validateEmail(string $email): string
    {
        $email = filter_var($email, FILTER_VALIDATE_EMAIL);
        
        if ($email === false) {
            throw new \InvalidArgumentException('Некорректный email');
        }
        
        return $email;
    }
    
    /**
     * Валидация enum значения
     */
    public function validateEnum($input, array $allowed)
    {
        if (!in_array($input, $allowed, true)) {
            throw new \InvalidArgumentException('Недопустимое значение');
        }
        
        return $input;
    }
    
    /**
     * Валидация массива ID
     */
    public function validateIdArray(array $ids, int $maxCount = 1000): array
    {
        if (count($ids) > $maxCount) {
            throw new \InvalidArgumentException('Слишком много элементов');
        }
        
        $result = [];
        foreach ($ids as $id) {
            $validId = $this->validateInt($id, 1, PHP_INT_MAX);
            $result[] = $validId;
        }
        
        return array_unique($result);
    }
}

/**
 * Класс для безопасной работы с сессиями
 */
class SecureSession
{
    /**
     * Генерация безопасного fingerprint
     */
    public static function generateFingerprint(): string
    {
        $data = [
            $_SERVER['HTTP_USER_AGENT'] ?? '',
            $_SERVER['HTTP_ACCEPT_LANGUAGE'] ?? '',
            $_SERVER['HTTP_ACCEPT_ENCODING'] ?? '',
            // Добавляем дополнительные параметры для усиления
            $_SERVER['HTTP_DNT'] ?? '',
            $_SERVER['HTTP_CONNECTION'] ?? '',
            // Используем только первые 3 октета IP для защиты от смены IP в одной подсети
            implode('.', array_slice(explode('.', $_SERVER['REMOTE_ADDR'] ?? ''), 0, 3))
        ];
        
        return hash('sha256', implode('|', $data));
    }
    
    /**
     * Проверка сессии на валидность
     */
    public static function validateSession(): bool
    {
        // Проверка fingerprint
        $currentFingerprint = self::generateFingerprint();
        if (!isset($_SESSION['fingerprint'])) {
            $_SESSION['fingerprint'] = $currentFingerprint;
        } elseif ($_SESSION['fingerprint'] !== $currentFingerprint) {
            // Возможная попытка угона сессии
            session_destroy();
            return false;
        }
        
        // Проверка времени жизни
        if (isset($_SESSION['last_activity'])) {
            $inactive = time() - $_SESSION['last_activity'];
            $maxInactive = ini_get('session.gc_maxlifetime') ?: 1440;
            
            if ($inactive > $maxInactive) {
                session_destroy();
                return false;
            }
        }
        
        $_SESSION['last_activity'] = time();
        
        // Регенерация ID сессии каждые 30 минут
        if (!isset($_SESSION['regenerated'])) {
            $_SESSION['regenerated'] = time();
        } elseif (time() - $_SESSION['regenerated'] > 1800) {
            session_regenerate_id(true);
            $_SESSION['regenerated'] = time();
        }
        
        return true;
    }
}

// Инициализация кеша при загрузке
Cache::init();