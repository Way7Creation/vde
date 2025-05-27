<?php
// Файл: src/Services/QueueService.php
namespace App\Services;

use App\Core\Database;

class QueueService
{
    const STATUS_PENDING = 'pending';
    const STATUS_PROCESSING = 'processing';
    const STATUS_COMPLETED = 'completed';
    const STATUS_FAILED = 'failed';
    
    /**
     * Добавить задачу в очередь
     * @param string $type Тип задачи (email, report, image_resize и т.д.)
     * @param array $payload Данные для выполнения
     * @param int $priority Приоритет (0 - низкий, 10 - высокий)
     */
    public static function push(string $type, array $payload, int $priority = 5): int
    {
        $pdo = Database::getConnection();
        $stmt = $pdo->prepare("
            INSERT INTO job_queue (type, payload, priority, status, created_at)
            VALUES (:type, :payload, :priority, :status, NOW())
        ");
        
        $stmt->execute([
            'type' => $type,
            'payload' => json_encode($payload, JSON_UNESCAPED_UNICODE),
            'priority' => $priority,
            'status' => self::STATUS_PENDING
        ]);
        
        return $pdo->lastInsertId();
    }
    
    /**
     * Получить следующую задачу для обработки
     */
    public static function pop(string $type = null): ?array
    {
        $pdo = Database::getConnection();
        
        // Начинаем транзакцию для атомарности
        $pdo->beginTransaction();
        
        try {
            $sql = "
                SELECT * FROM job_queue 
                WHERE status = :status
                " . ($type ? "AND type = :type" : "") . "
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                FOR UPDATE
            ";
            
            $stmt = $pdo->prepare($sql);
            $params = ['status' => self::STATUS_PENDING];
            if ($type) $params['type'] = $type;
            
            $stmt->execute($params);
            $job = $stmt->fetch();
            
            if (!$job) {
                $pdo->rollback();
                return null;
            }
            
            // Помечаем как обрабатываемую
            $updateStmt = $pdo->prepare("
                UPDATE job_queue 
                SET status = :status, started_at = NOW() 
                WHERE id = :id
            ");
            
            $updateStmt->execute([
                'status' => self::STATUS_PROCESSING,
                'id' => $job['id']
            ]);
            
            $pdo->commit();
            
            // Декодируем payload
            $job['payload'] = json_decode($job['payload'], true);
            
            return $job;
            
        } catch (\Exception $e) {
            $pdo->rollback();
            throw $e;
        }
    }
    
    /**
     * Отметить задачу как выполненную
     */
    public static function complete(int $jobId, array $result = []): void
    {
        $pdo = Database::getConnection();
        $stmt = $pdo->prepare("
            UPDATE job_queue 
            SET status = :status, 
                completed_at = NOW(),
                result = :result
            WHERE id = :id
        ");
        
        $stmt->execute([
            'status' => self::STATUS_COMPLETED,
            'result' => json_encode($result, JSON_UNESCAPED_UNICODE),
            'id' => $jobId
        ]);
    }
    
    /**
     * Отметить задачу как проваленную
     */
    public static function fail(int $jobId, string $error, bool $retry = true): void
    {
        $pdo = Database::getConnection();
        
        if ($retry) {
            // Увеличиваем счетчик попыток
            $stmt = $pdo->prepare("
                UPDATE job_queue 
                SET status = :status,
                    attempts = attempts + 1,
                    last_error = :error
                WHERE id = :id AND attempts < 3
            ");
        } else {
            $stmt = $pdo->prepare("
                UPDATE job_queue 
                SET status = :failed_status,
                    failed_at = NOW(),
                    last_error = :error
                WHERE id = :id
            ");
        }
        
        $stmt->execute([
            'status' => self::STATUS_PENDING,
            'failed_status' => self::STATUS_FAILED,
            'error' => $error,
            'id' => $jobId
        ]);
    }
}
