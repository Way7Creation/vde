<?php
namespace App\Core;

class Logger
{
    private const LEVELS = [
        'emergency' => 0, 'alert' => 1, 'critical' => 2, 'error' => 3,
        'warning' => 4, 'notice' => 5, 'info' => 6, 'debug' => 7
    ];

    public static function emergency(string $message, array $context = []): void
    {
        self::log('emergency', $message, $context);
    }

    public static function error(string $message, array $context = []): void
    {
        self::log('error', $message, $context);
    }

    public static function warning(string $message, array $context = []): void
    {
        self::log('warning', $message, $context);
    }

    public static function info(string $message, array $context = []): void
    {
        self::log('info', $message, $context);
    }

    public static function security(string $message, array $context = []): void
    {
        $context['security_event'] = true;
        self::log('warning', "[SECURITY] {$message}", $context);
    }

    private static function log(string $level, string $message, array $context): void
    {
        try {
            // Логируем в базу данных
            $extra = [
                'user_id' => $_SESSION['user_id'] ?? null,
                'ip' => $_SERVER['REMOTE_ADDR'] ?? '',
                'user_agent' => $_SERVER['HTTP_USER_AGENT'] ?? '',
                'uri' => $_SERVER['REQUEST_URI'] ?? '',
                'method' => $_SERVER['REQUEST_METHOD'] ?? ''
            ];

            Database::query(
                "INSERT INTO application_logs (level, message, context, extra, created_at) 
                 VALUES (?, ?, ?, ?, NOW())",
                [$level, $message, json_encode($context), json_encode($extra)]
            );

        } catch (\Exception $e) {
            // Fallback на файловое логирование
            error_log("Logger failed: " . $e->getMessage());
            error_log("Original log: [{$level}] {$message}");
        }

        // Дублируем критичные события в файл
        if (in_array($level, ['emergency', 'alert', 'critical', 'error'])) {
            self::logToFile($level, $message, $context);
        }
    }

    private static function logToFile(string $level, string $message, array $context): void
    {
        $logDir = '/var/www/logs';
        if (!is_dir($logDir)) {
            mkdir($logDir, 0755, true);
        }

        $logFile = $logDir . '/error.log';
        $timestamp = date('Y-m-d H:i:s');
        $contextStr = !empty($context) ? ' ' . json_encode($context) : '';
        
        $logLine = "[{$timestamp}] {$level}: {$message}{$contextStr}" . PHP_EOL;
        file_put_contents($logFile, $logLine, FILE_APPEND | LOCK_EX);
    }
}