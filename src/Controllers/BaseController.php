<?php
namespace App\Controllers;

use App\Core\Logger;
use App\Core\Validator;
use App\Exceptions\ValidationException;

/**
 * Базовый контроллер с общей функциональностью
 */
abstract class BaseController
{
    protected array $data = [];
    protected int $statusCode = 200;
    
    /**
     * Валидация входных данных
     */
    protected function validate(array $data, array $rules): array
    {
        $validator = new Validator($data, $rules);
        
        if (!$validator->passes()) {
            throw new ValidationException("Validation failed", $validator->errors());
        }
        
        return $validator->validated();
    }

    /**
     * JSON ответ
     */
    protected function jsonResponse(array $data, int $statusCode = 200): void
    {
        http_response_code($statusCode);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode($data, JSON_UNESCAPED_UNICODE);
        exit;
    }

    /**
     * Успешный ответ
     */
    protected function success($data = null, string $message = 'Success'): void
    {
        $response = [
            'success' => true,
            'message' => $message
        ];
        
        if ($data !== null) {
            $response['data'] = $data;
        }
        
        $this->jsonResponse($response);
    }

    /**
     * Ответ с ошибкой
     */
    protected function error(string $message, int $statusCode = 400, array $errors = []): void
    {
        $response = [
            'success' => false,
            'message' => $message
        ];
        
        if (!empty($errors)) {
            $response['errors'] = $errors;
        }
        
        Logger::warning("Controller error response", [
            'message' => $message,
            'status_code' => $statusCode,
            'errors' => $errors
        ]);
        
        $this->jsonResponse($response, $statusCode);
    }

    /**
     * Получить входные данные
     */
    protected function getInput(): array
    {
        $contentType = $_SERVER['CONTENT_TYPE'] ?? '';
        
        if (strpos($contentType, 'application/json') !== false) {
            $input = json_decode(file_get_contents('php://input'), true);
            return $input ?: [];
        }
        
        return array_merge($_GET, $_POST);
    }

    /**
     * Проверка аутентификации
     */
    protected function requireAuth(): array
    {
        if (!AuthService::validateSession()) {
            $this->error('Authentication required', 401);
        }
        
        return [
            'user_id' => $_SESSION['user_id'],
            'username' => $_SESSION['username'],
            'role' => $_SESSION['role']
        ];
    }

    /**
     * Проверка прав доступа
     */
    protected function requireRole(string $role): void
    {
        $user = $this->requireAuth();
        
        if ($user['role'] !== $role && $user['role'] !== 'admin') {
            $this->error('Insufficient permissions', 403);
        }
    }
}