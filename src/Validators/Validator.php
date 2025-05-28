<?php
namespace App\Validators;

use App\Exceptions\ValidationException;

/**
 * Мощная система валидации данных
 */
class Validator
{
    private array $data;
    private array $rules;
    private array $errors = [];
    private array $validated = [];

    public function __construct(array $data, array $rules)
    {
        $this->data = $data;
        $this->rules = $rules;
    }

    /**
     * Выполнить валидацию
     */
    public function validate(): bool
    {
        $this->errors = [];
        $this->validated = [];

        foreach ($this->rules as $field => $rules) {
            $this->validateField($field, $rules);
        }

        return empty($this->errors);
    }

    /**
     * Проверить, прошла ли валидация
     */
    public function passes(): bool
    {
        return $this->validate();
    }

    /**
     * Проверить, не прошла ли валидация
     */
    public function fails(): bool
    {
        return !$this->passes();
    }

    /**
     * Получить ошибки валидации
     */
    public function errors(): array
    {
        return $this->errors;
    }

    /**
     * Получить провалидированные данные
     */
    public function validated(): array
    {
        if (!$this->passes()) {
            throw new ValidationException("Cannot get validated data before successful validation");
        }
        return $this->validated;
    }

    /**
     * Валидация отдельного поля
     */
    private function validateField(string $field, $rules): void
    {
        if (is_string($rules)) {
            $rules = explode('|', $rules);
        }

        $value = $this->data[$field] ?? null;
        $isRequired = in_array('required', $rules);

        // Если поле обязательное и пустое
        if ($isRequired && $this->isEmpty($value)) {
            $this->addError($field, "Field {$field} is required");
            return;
        }

        // Если поле необязательное и пустое, пропускаем остальные правила
        if (!$isRequired && $this->isEmpty($value)) {
            return;
        }

        // Применяем правила валидации
        foreach ($rules as $rule) {
            if ($rule === 'required') continue;

            $this->applyRule($field, $value, $rule);
        }

        // Если нет ошибок, добавляем в провалидированные данные
        if (!isset($this->errors[$field])) {
            $this->validated[$field] = $value;
        }
    }

    /**
     * Применить конкретное правило валидации
     */
    private function applyRule(string $field, $value, string $rule): void
    {
        // Разбираем правило с параметрами (например, min:3)
        $parts = explode(':', $rule, 2);
        $ruleName = $parts[0];
        $parameter = $parts[1] ?? null;

        switch ($ruleName) {
            case 'string':
                if (!is_string($value)) {
                    $this->addError($field, "Field {$field} must be a string");
                }
                break;

            case 'integer':
            case 'int':
                if (!is_numeric($value) || (int)$value != $value) {
                    $this->addError($field, "Field {$field} must be an integer");
                }
                break;

            case 'email':
                if (!filter_var($value, FILTER_VALIDATE_EMAIL)) {
                    $this->addError($field, "Field {$field} must be a valid email address");
                }
                break;

            case 'min':
                if ($parameter === null) {
                    $this->addError($field, "Min rule requires a parameter");
                    break;
                }
                
                if (is_string($value) && strlen($value) < (int)$parameter) {
                    $this->addError($field, "Field {$field} must be at least {$parameter} characters");
                } elseif (is_numeric($value) && $value < (float)$parameter) {
                    $this->addError($field, "Field {$field} must be at least {$parameter}");
                }
                break;

            case 'max':
                if ($parameter === null) {
                    $this->addError($field, "Max rule requires a parameter");
                    break;
                }
                
                if (is_string($value) && strlen($value) > (int)$parameter) {
                    $this->addError($field, "Field {$field} may not be greater than {$parameter} characters");
                } elseif (is_numeric($value) && $value > (float)$parameter) {
                    $this->addError($field, "Field {$field} may not be greater than {$parameter}");
                }
                break;

            case 'regex':
                if ($parameter === null) {
                    $this->addError($field, "Regex rule requires a parameter");
                    break;
                }
                
                if (!preg_match($parameter, $value)) {
                    $this->addError($field, "Field {$field} format is invalid");
                }
                break;

            case 'in':
                if ($parameter === null) {
                    $this->addError($field, "In rule requires a parameter");
                    break;
                }
                
                $allowedValues = explode(',', $parameter);
                if (!in_array($value, $allowedValues)) {
                    $this->addError($field, "Field {$field} must be one of: " . implode(', ', $allowedValues));
                }
                break;

            case 'unique':
                // Проверка уникальности в БД
                if ($parameter === null) {
                    $this->addError($field, "Unique rule requires a table parameter");
                    break;
                }
                
                if ($this->isNotUnique($parameter, $field, $value)) {
                    $this->addError($field, "Field {$field} must be unique");
                }
                break;

            case 'exists':
                // Проверка существования в БД
                if ($parameter === null) {
                    $this->addError($field, "Exists rule requires a table parameter");
                    break;
                }
                
                if (!$this->existsInDatabase($parameter, $field, $value)) {
                    $this->addError($field, "Selected {$field} is invalid");
                }
                break;

            default:
                $this->addError($field, "Unknown validation rule: {$ruleName}");
        }
    }

    /**
     * Добавить ошибку валидации
     */
    private function addError(string $field, string $message): void
    {
        if (!isset($this->errors[$field])) {
            $this->errors[$field] = [];
        }
        $this->errors[$field][] = $message;
    }

    /**
     * Проверить, пустое ли значение
     */
    private function isEmpty($value): bool
    {
        return $value === null || $value === '' || (is_array($value) && empty($value));
    }

    /**
     * Проверить уникальность в базе данных
     */
    private function isNotUnique(string $table, string $field, $value): bool
    {
        try {
            $stmt = \App\Core\Database::query(
                "SELECT COUNT(*) FROM {$table} WHERE {$field} = ?",
                [$value]
            );
            
            return $stmt->fetchColumn() > 0;
            
        } catch (\Exception $e) {
            error_log("Unique validation error: " . $e->getMessage());
            return false; // В случае ошибки считаем уникальным
        }
    }

    /**
     * Проверить существование в базе данных
     */
    private function existsInDatabase(string $table, string $field, $value): bool
    {
        try {
            $stmt = \App\Core\Database::query(
                "SELECT COUNT(*) FROM {$table} WHERE {$field} = ?",
                [$value]
            );
            
            return $stmt->fetchColumn() > 0;
            
        } catch (\Exception $e) {
            error_log("Exists validation error: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Статический метод для быстрой валидации
     */
    public static function make(array $data, array $rules): self
    {
        return new self($data, $rules);
    }
}