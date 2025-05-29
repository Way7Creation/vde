<?php
/**
 * Упрощенный API для работы с товарами
 * Использует уже работающий get_protop.php
 */

header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

// Обработка CORS preflight
if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(204);
    exit;
}

ini_set('display_errors', 0);
ini_set('log_errors', 1);
error_reporting(E_ALL);

try {
    $action = $_GET['action'] ?? 'search';
    
    switch ($action) {
        case 'search':
            // Преобразуем параметры для get_protop.php
            $_GET['page'] = $_GET['page'] ?? 1;
            $_GET['itemsPerPage'] = $_GET['limit'] ?? 20;
            $_GET['sortColumn'] = $_GET['sort'] ?? 'name';
            $_GET['sortDirection'] = 'asc';
            
            // Обрабатываем фильтры
            $filters = [];
            if (!empty($_GET['filters'])) {
                $filters = json_decode($_GET['filters'], true) ?: [];
            }
            if (!empty($_GET['q'])) {
                $filters['search'] = $_GET['q'];
            }
            $_GET['filters'] = json_encode($filters);
            
            // Подключаем get_protop.php и перехватываем вывод
            ob_start();
            require __DIR__ . '/../get_protop.php';
            $output = ob_get_clean();
            
            // Декодируем результат
            $data = json_decode($output, true);
            
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new Exception('Invalid response from get_protop.php');
            }
            
            // Преобразуем в наш формат
            $result = [
                'success' => true,
                'data' => [
                    'products' => $data['products'] ?? [],
                    'total' => $data['totalProducts'] ?? 0,
                    'page' => (int)($_GET['page'] ?? 1),
                    'limit' => (int)($_GET['limit'] ?? 20),
                    'pages' => ceil(($data['totalProducts'] ?? 0) / ($_GET['limit'] ?? 20))
                ],
                'aggregations' => $data['aggregations'] ?? [],
                'query_info' => $data['query_info'] ?? []
            ];
            
            echo json_encode($result, JSON_UNESCAPED_UNICODE | JSON_NUMERIC_CHECK);
            break;
            
        case 'get':
            // Получение одного товара
            $id = $_GET['id'] ?? null;
            if (!$id) {
                throw new Exception('Product ID required', 400);
            }
            
            // Используем get_protop.php для поиска по ID
            $_GET['filters'] = json_encode(['search' => $id]);
            $_GET['page'] = 1;
            $_GET['itemsPerPage'] = 1;
            
            ob_start();
            require __DIR__ . '/../get_protop.php';
            $output = ob_get_clean();
            
            $data = json_decode($output, true);
            $products = $data['products'] ?? [];
            
            if (empty($products)) {
                throw new Exception('Product not found', 404);
            }
            
            echo json_encode([
                'success' => true,
                'data' => $products[0]
            ], JSON_UNESCAPED_UNICODE | JSON_NUMERIC_CHECK);
            break;
            
        case 'autocomplete':
            // Автодополнение - используем поиск с лимитом
            $query = trim($_GET['q'] ?? '');
            if (strlen($query) < 2) {
                echo json_encode(['success' => true, 'suggestions' => []]);
                exit;
            }
            
            $_GET['filters'] = json_encode(['search' => $query]);
            $_GET['page'] = 1;
            $_GET['itemsPerPage'] = 10;
            
            ob_start();
            require __DIR__ . '/../get_protop.php';
            $output = ob_get_clean();
            
            $data = json_decode($output, true);
            $products = $data['products'] ?? [];
            
            // Формируем suggestions
            $suggestions = [];
            $seen = [];
            
            foreach ($products as $product) {
                // По названию
                if (!empty($product['name']) && !isset($seen[$product['name']])) {
                    $suggestions[] = [
                        'text' => $product['name'],
                        'type' => 'text'
                    ];
                    $seen[$product['name']] = true;
                }
                
                // По коду
                if (!empty($product['external_id']) && !isset($seen[$product['external_id']])) {
                    $suggestions[] = [
                        'text' => $product['external_id'],
                        'type' => 'code'
                    ];
                    $seen[$product['external_id']] = true;
                }
                
                if (count($suggestions) >= 5) break;
            }
            
            echo json_encode([
                'success' => true,
                'suggestions' => $suggestions
            ], JSON_UNESCAPED_UNICODE);
            break;
            
        default:
            throw new Exception('Unknown action', 400);
    }
    
} catch (Exception $e) {
    http_response_code($e->getCode() ?: 500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage()
    ], JSON_UNESCAPED_UNICODE);
}