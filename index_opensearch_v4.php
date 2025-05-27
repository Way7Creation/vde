<?php
/**
 * Скрипт индексации товаров в OpenSearch v4
 * ТОЛЬКО СТАТИЧЕСКИЕ ДАННЫЕ!
 * Цены и остатки загружаются отдельно через API
 */

require __DIR__ . '/vendor/autoload.php';

use OpenSearch\ClientBuilder;

ini_set('memory_limit', '2G');
set_time_limit(0);

// Класс для безопасной работы с БД
class DatabaseConnection {
    private static ?PDO $instance = null;
    
    public static function getInstance(): PDO {
        if (self::$instance === null) {
            $config = parse_ini_file('/var/www/www-root/data/config/config_bd.ini', true)['mysql'];
            $dsn = "mysql:host={$config['host']};dbname={$config['database']};charset=utf8mb4";
            
            self::$instance = new PDO($dsn, $config['user'], $config['password'], [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => false // Для больших выборок
            ]);
        }
        return self::$instance;
    }
}

// Функция нормализации текста
function normalizeText($str): string {
    if (!is_string($str)) return '';
    
    // Убираем управляющие символы и нормализуем пробелы
    $str = preg_replace('/[^\P{C}\t\n\r]+/u', '', $str ?? '');
    $str = preg_replace('/\s+/', ' ', $str);
    
    return trim($str);
}

// Функция создания подсказок для автодополнения
function createSuggestData(array $product): array {
    $suggestions = [];
    
    // Название товара - высший приоритет
    if (!empty($product['name'])) {
        $suggestions[] = [
            'input' => [$product['name']],
            'weight' => 100
        ];
        
        // Добавляем первые слова названия
        $words = explode(' ', $product['name']);
        if (count($words) > 2) {
            $suggestions[] = [
                'input' => [implode(' ', array_slice($words, 0, 2))],
                'weight' => 80
            ];
        }
    }
    
    // Коды товара
    if (!empty($product['external_id'])) {
        $suggestions[] = [
            'input' => [
                $product['external_id'],
                str_replace(['-', '_', '/', ' '], '', $product['external_id'])
            ],
            'weight' => 90
        ];
    }
    
    // Бренд
    if (!empty($product['brand_name'])) {
        $suggestions[] = [
            'input' => [$product['brand_name']],
            'weight' => 70
        ];
    }
    
    return $suggestions;
}

try {
    echo "=== Индексация товаров OpenSearch v4 ===\n\n";
    
    // Подключение к OpenSearch
    $client = ClientBuilder::create()
        ->setHosts(['localhost:9200'])
        ->build();
    
    // Проверка соединения
    $info = $client->info();
    echo "OpenSearch версия: " . $info['version']['number'] . "\n\n";
    
    // Удаляем старый индекс
    echo "Удаление старого индекса...\n";
    try {
        $client->indices()->delete(['index' => 'products_v4']);
        echo "Индекс удален\n";
    } catch (\Exception $e) {
        echo "Индекс не существует\n";
    }
    
    // Создаем новый индекс
    echo "Создание нового индекса products_v4...\n";
    $indexConfig = json_decode(file_get_contents(__DIR__ . '/products_v4.json'), true);
    $client->indices()->create([
        'index' => 'products_v4',
        'body' => $indexConfig
    ]);
    echo "Индекс создан успешно\n\n";
    
    // Подключение к БД
    $pdo = DatabaseConnection::getInstance();
    
    // Получаем общее количество товаров
    $totalCount = $pdo->query("SELECT COUNT(*) FROM products")->fetchColumn();
    echo "Найдено товаров для индексации: $totalCount\n\n";
    
    // Индексируем батчами по 500 товаров
    $batchSize = 500;
    $processed = 0;
    $errors = 0;
    $startTime = microtime(true);
    
    // Подготавливаем запросы для дополнительных данных
    $stmtAttributes = $pdo->prepare("
        SELECT name, value, unit 
        FROM product_attributes 
        WHERE product_id = :product_id 
        ORDER BY sort_order
    ");
    
    $stmtCategories = $pdo->prepare("
        SELECT c.category_id, c.name, c.slug
        FROM product_categories pc
        JOIN categories c ON pc.category_id = c.category_id
        WHERE pc.product_id = :product_id
    ");
    
    $stmtImages = $pdo->prepare("
        SELECT url, alt_text, is_main
        FROM product_images
        WHERE product_id = :product_id
        ORDER BY is_main DESC, sort_order
    ");
    
    $stmtDocuments = $pdo->prepare("
        SELECT type, COUNT(*) as count
        FROM product_documents
        WHERE product_id = :product_id
        GROUP BY type
    ");
    
    // Основной запрос товаров
    $stmt = $pdo->query("
        SELECT 
            p.product_id,
            p.external_id,
            p.sku,
            p.name,
            p.description,
            p.brand_id,
            b.name AS brand_name,
            p.series_id,
            s.name AS series_name,
            p.unit,
            p.min_sale,
            p.weight,
            p.dimensions,
            p.created_at,
            p.updated_at
        FROM products p
        LEFT JOIN brands b ON p.brand_id = b.brand_id
        LEFT JOIN series s ON p.series_id = s.series_id
        ORDER BY p.product_id
    ");
    
    $bulkData = [];
    
    while ($product = $stmt->fetch()) {
        try {
            // Нормализация основных данных
            foreach (['external_id', 'sku', 'name', 'description', 'brand_name', 'series_name'] as $field) {
                if (isset($product[$field])) {
                    $product[$field] = normalizeText($product[$field]);
                }
            }
            
            // Получаем атрибуты
            $stmtAttributes->execute(['product_id' => $product['product_id']]);
            $attributes = $stmtAttributes->fetchAll();
            $product['attributes'] = array_map(function($attr) {
                return [
                    'name' => normalizeText($attr['name']),
                    'value' => normalizeText($attr['value']),
                    'unit' => $attr['unit'] ?? ''
                ];
            }, $attributes);
            
            // Получаем категории
            $stmtCategories->execute(['product_id' => $product['product_id']]);
            $categories = $stmtCategories->fetchAll();
            $product['categories'] = implode(', ', array_column($categories, 'name'));
            $product['category_ids'] = array_column($categories, 'category_id');
            
            // Получаем изображения (только URL)
            $stmtImages->execute(['product_id' => $product['product_id']]);
            $images = $stmtImages->fetchAll();
            $product['images'] = array_column($images, 'url');
            
            // Получаем документы (только количество по типам)
            $stmtDocuments->execute(['product_id' => $product['product_id']]);
            $docs = $stmtDocuments->fetchAll(PDO::FETCH_KEY_PAIR);
            $product['documents'] = [
                'certificates' => $docs['certificate'] ?? 0,
                'manuals' => $docs['manual'] ?? 0,
                'drawings' => $docs['drawing'] ?? 0
            ];
            
            // Форматируем даты
            $product['created_at'] = date('Y-m-d\TH:i:s', strtotime($product['created_at']));
            $product['updated_at'] = date('Y-m-d\TH:i:s', strtotime($product['updated_at']));
            
            // Создаем данные для автодополнения
            $product['suggest'] = createSuggestData($product);
            
            // Добавляем в bulk массив
            $bulkData[] = ['index' => ['_index' => 'products_v4', '_id' => $product['product_id']]];
            $bulkData[] = $product;
            
            // Отправляем batch
            if (count($bulkData) >= $batchSize * 2) {
                $response = $client->bulk(['body' => $bulkData]);
                
                if (isset($response['errors']) && $response['errors']) {
                    foreach ($response['items'] as $item) {
                        if (isset($item['index']['error'])) {
                            error_log('Ошибка индексации: ' . json_encode($item['index']['error']));
                            $errors++;
                        }
                    }
                }
                
                $processed += count($bulkData) / 2;
                $progress = round(($processed / $totalCount) * 100, 1);
                echo "\rПрогресс: $progress% ($processed из $totalCount)";
                
                $bulkData = [];
            }
            
        } catch (\Exception $e) {
            error_log("Ошибка обработки товара ID {$product['product_id']}: " . $e->getMessage());
            $errors++;
        }
    }
    
    // Отправляем оставшиеся данные
    if (!empty($bulkData)) {
        $response = $client->bulk(['body' => $bulkData]);
        $processed += count($bulkData) / 2;
    }
    
    // Обновляем индекс
    echo "\n\nОбновление индекса...\n";
    $client->indices()->refresh(['index' => 'products_v4']);
    
    // Создаем алиас
    echo "Создание алиаса products_current...\n";
    try {
        $client->indices()->deleteAlias(['index' => '_all', 'name' => 'products_current']);
    } catch (\Exception $e) {
        // Алиаса не было
    }
    
    $client->indices()->putAlias([
        'index' => 'products_v4',
        'name' => 'products_current'
    ]);
    
    $totalTime = microtime(true) - $startTime;
    
    echo "\n\n=== ИНДЕКСАЦИЯ ЗАВЕРШЕНА ===\n";
    echo "Обработано: $processed товаров\n";
    echo "Ошибок: $errors\n";
    echo "Время: " . number_format($totalTime, 2) . " сек\n";
    echo "Скорость: " . number_format($processed / $totalTime, 0) . " товаров/сек\n";
    
} catch (\Exception $e) {
    echo "\n\nКРИТИЧЕСКАЯ ОШИБКА: " . $e->getMessage() . "\n";
    echo "Trace: " . $e->getTraceAsString() . "\n";
    exit(1);
}
