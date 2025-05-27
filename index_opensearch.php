<?php
/**
 * Скрипт полной переиндексации товаров в OpenSearch v3
 * С улучшенной обработкой для умного поиска
 * 
 * Запускать из командной строки: php index_opensearch_v3.php
 */

header('Content-Type: text/plain; charset=utf-8');
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);
ini_set('memory_limit', '12G');
set_time_limit(0);

require __DIR__ . '/vendor/autoload.php';
use OpenSearch\ClientBuilder;

// Функция нормализации текста
function normalizeText($str) {
    if (!is_string($str)) return '';
    
    // Привести к UTF-8
    if (!mb_check_encoding($str, 'UTF-8')) {
        $str = mb_convert_encoding($str, 'UTF-8', 'auto');
    }
    
    // Удаляем эмодзи и спецсимволы
    $str = preg_replace('/[\x{1F600}-\x{1F64F}]/u', '', $str);
    $str = preg_replace('/[\x{1F300}-\x{1F5FF}]/u', '', $str);
    $str = preg_replace('/[\x{1F680}-\x{1F6FF}]/u', '', $str);
    $str = preg_replace('/[\x{2600}-\x{26FF}]/u', '', $str);
    
    // Убираем управляющие символы
    $str = preg_replace('/[^\P{C}\t\n\r]+/u', '', $str ?? '');
    
    // Нормализуем пробелы
    $str = preg_replace('/\s+/', ' ', $str);
    
    return trim($str);
}

// Функция для создания поискового текста
function createSearchText($product, $attributes) {
    $parts = [];
    
    // Основные поля
    $fields = ['name', 'description', 'sku', 'external_id', 'brand_name', 'series_name', 'categories'];
    foreach ($fields as $field) {
        if (!empty($product[$field])) {
            $parts[] = $product[$field];
        }
    }
    
    // Атрибуты
    foreach ($attributes as $attr) {
        $parts[] = $attr['name'] . ' ' . $attr['value'];
        // Добавляем отдельно значение для поиска по характеристикам
        if (preg_match('/\d+/', $attr['value'])) {
            $parts[] = $attr['value'];
        }
    }
    
    // Создаем варианты написания кода товара
    if (!empty($product['external_id'])) {
        $code = $product['external_id'];
        // Без дефисов
        $parts[] = str_replace(['-', '_', '/', ' '], '', $code);
        // С пробелами
        $parts[] = str_replace(['-', '_', '/'], ' ', $code);
        // Каждая часть отдельно
        $codeParts = preg_split('/[-_\/\s]+/', $code);
        $parts = array_merge($parts, $codeParts);
    }
    
    // То же для SKU
    if (!empty($product['sku'])) {
        $sku = $product['sku'];
        $parts[] = str_replace(['-', '_', '/', ' '], '', $sku);
        $parts[] = str_replace(['-', '_', '/'], ' ', $sku);
    }
    
    return normalizeText(implode(' ', array_unique($parts)));
}

// Подключение к OpenSearch
echo "Подключение к OpenSearch...\n";
$client = ClientBuilder::create()
    ->setHosts(['localhost:9200'])
    ->build();

// Проверка соединения
try {
    $info = $client->info();
    echo "OpenSearch версия: " . $info['version']['number'] . "\n\n";
} catch (\Exception $e) {
    die("Ошибка подключения к OpenSearch: " . $e->getMessage() . "\n");
}

// Подключение к БД
$config_path = '/var/www/www-root/data/config/config_bd.ini';
if (!file_exists($config_path)) {
    die("Конфиг не найден: $config_path\n");
}

$config_all = parse_ini_file($config_path, true, INI_SCANNER_NORMAL);
if ($config_all === false || !isset($config_all['mysql'])) {
    die("Ошибка чтения конфигурации БД\n");
}

$config = array_map(function($v){ return trim($v, "\"'"); }, $config_all['mysql']);
$dsn = "mysql:host={$config['host']};dbname={$config['database']};charset=utf8mb4";

try {
    $pdo = new PDO($dsn, $config['user'], $config['password'], [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);
    echo "Подключение к БД успешно\n\n";
} catch (PDOException $e) {
    die("Ошибка подключения к БД: " . $e->getMessage() . "\n");
}

// Удаляем старый индекс
echo "Удаление старого индекса products_v3...\n";
try {
    $client->indices()->delete(['index' => 'products_v3']);
    echo "Индекс удален\n";
} catch (\Exception $e) {
    echo "Индекс не существует или ошибка: " . $e->getMessage() . "\n";
}

// Создаем новый индекс
echo "\nСоздание нового индекса products_v3...\n";
$indexConfigPath = __DIR__ . '/products_v3.json';
if (!file_exists($indexConfigPath)) {
    die("Файл конфигурации индекса не найден: $indexConfigPath\n");
}

$indexConfig = json_decode(file_get_contents($indexConfigPath), true);
if (json_last_error() !== JSON_ERROR_NONE) {
    die("Ошибка парсинга JSON конфигурации: " . json_last_error_msg() . "\n");
}

try {
    $response = $client->indices()->create([
        'index' => 'products_v3',
        'body' => $indexConfig
    ]);
    echo "Индекс создан успешно\n\n";
} catch (\Exception $e) {
    die("Ошибка создания индекса: " . $e->getMessage() . "\n");
}

// Получаем товары
echo "Загрузка товаров из БД...\n";
$sql_products = "
    SELECT
        p.product_id,
        p.external_id,
        p.sku,
        p.name,
        p.description,
        p.unit,
        p.min_sale,
        p.weight,
        p.dimensions,
        p.created_at,
        p.updated_at,
        b.name AS brand_name,
        s.name AS series_name,
        GROUP_CONCAT(DISTINCT pi.url SEPARATOR '|') AS image_urls,
        GROUP_CONCAT(DISTINCT c.name SEPARATOR ', ') AS categories,
        GROUP_CONCAT(DISTINCT c.slug SEPARATOR ',') AS category_slugs
    FROM products p
    LEFT JOIN brands b ON p.brand_id = b.brand_id
    LEFT JOIN series s ON p.series_id = s.series_id
    LEFT JOIN product_images pi ON p.product_id = pi.product_id
    LEFT JOIN product_categories pc ON p.product_id = pc.product_id
    LEFT JOIN categories c ON pc.category_id = c.category_id
    GROUP BY p.product_id
";

$stmt_products = $pdo->prepare($sql_products);
$stmt_products->execute();
$products = $stmt_products->fetchAll(PDO::FETCH_ASSOC);

echo "Найдено товаров: " . count($products) . "\n\n";

if (empty($products)) {
    die("Нет товаров для индексации\n");
}

// Подготовка запросов для дополнительных данных
$stmt_attributes = $pdo->prepare("
    SELECT name, value, unit 
    FROM product_attributes 
    WHERE product_id = :product_id 
    ORDER BY sort_order
");

$stmt_base_price = $pdo->prepare("
    SELECT price 
    FROM prices 
    WHERE product_id = :product_id 
    AND is_base = 1 
    ORDER BY valid_from DESC 
    LIMIT 1
");

$stmt_stock = $pdo->prepare("
    SELECT SUM(quantity - reserved) as total_stock
    FROM stock_balances 
    WHERE product_id = :product_id
");

$stmt_documents = $pdo->prepare("
    SELECT type, COUNT(*) as count
    FROM product_documents 
    WHERE product_id = :product_id
    GROUP BY type
");

// Начинаем индексацию
echo "Начинаем индексацию товаров...\n";
echo "=====================================\n";

$totalProducts = count($products);
$processed = 0;
$errors = 0;
$bulkData = [];
$startTime = microtime(true);

foreach ($products as $product) {
    $product_id = $product['product_id'];
    
    try {
        // Нормализация данных
        foreach ($product as $k => $v) {
            if (in_array($k, ['created_at','updated_at','min_sale','weight','product_id'])) continue;
            $product[$k] = normalizeText($v);
        }
        
        // Форматирование дат
        $product['created_at'] = !empty($product['created_at']) 
            ? date('Y-m-d H:i:s', strtotime($product['created_at'])) 
            : date('Y-m-d H:i:s');
        $product['updated_at'] = !empty($product['updated_at']) 
            ? date('Y-m-d H:i:s', strtotime($product['updated_at'])) 
            : date('Y-m-d H:i:s');
        
        // Получаем атрибуты
        $stmt_attributes->execute(['product_id' => $product_id]);
        $attributes = $stmt_attributes->fetchAll(PDO::FETCH_ASSOC);
        
        $product['attributes'] = array_map(function($attr) {
            return [
                'name' => normalizeText($attr['name']),
                'value' => normalizeText($attr['value']),
                'unit' => normalizeText($attr['unit'] ?? '')
            ];
        }, $attributes);
        
        // Получаем цены
        $stmt_base_price->execute(['product_id' => $product_id]);
        $basePrice = $stmt_base_price->fetchColumn();
        $product['base_price'] = $basePrice ? (float)$basePrice : 0;
        $product['retail_price'] = round($product['base_price'] * 1.2, 2);
        $product['price_range'] = $product['base_price'] > 0 ? 
            (floor($product['base_price'] / 100) * 100) : 0;
        
        // Получаем остатки
        $stmt_stock->execute(['product_id' => $product_id]);
        $stock = $stmt_stock->fetchColumn();
        $product['stock_total'] = $stock ? (int)$stock : 0;
        $product['in_stock'] = $product['stock_total'] > 0;
        
        // Получаем документы
        $stmt_documents->execute(['product_id' => $product_id]);
        $docs = $stmt_documents->fetchAll(PDO::FETCH_KEY_PAIR);
        $product['has_certificate'] = isset($docs['certificate']) && $docs['certificate'] > 0;
        $product['has_manual'] = isset($docs['manual']) && $docs['manual'] > 0;
        $product['has_drawing'] = isset($docs['drawing']) && $docs['drawing'] > 0;
        
        // Создаем поисковый текст
        $product['search_text'] = createSearchText($product, $attributes);
        
        // Подготовка данных для автодополнения
        $suggestInputs = [];
        
        // Название товара - высший приоритет
        if (!empty($product['name'])) {
            $suggestInputs[] = [
                'input' => [$product['name']],
                'weight' => 100
            ];
            
            // Добавляем части названия для автодополнения
            $nameParts = preg_split('/\s+/', $product['name']);
            if (count($nameParts) > 2) {
                foreach ($nameParts as $part) {
                    if (mb_strlen($part) > 3) {
                        $suggestInputs[] = [
                            'input' => [$part],
                            'weight' => 50
                        ];
                    }
                }
            }
        }
        
        // Код товара
        if (!empty($product['external_id'])) {
            $suggestInputs[] = [
                'input' => [
                    $product['external_id'],
                    str_replace(['-', '_', '/'], '', $product['external_id'])
                ],
                'weight' => 90
            ];
        }
        
        // SKU
        if (!empty($product['sku'])) {
            $suggestInputs[] = [
                'input' => [
                    $product['sku'],
                    str_replace(['-', '_', '/'], '', $product['sku'])
                ],
                'weight' => 80
            ];
        }
        
        // Бренд
        if (!empty($product['brand_name'])) {
            $suggestInputs[] = [
                'input' => [$product['brand_name']],
                'weight' => 70
            ];
        }
        
        $product['suggest'] = $suggestInputs;
        
        // Добавляем в bulk массив
        $bulkData[] = ['index' => ['_index' => 'products_v3', '_id' => $product_id]];
        $bulkData[] = $product;
        
        // Отправляем batch каждые 500 товаров
        if (count($bulkData) >= 500) { // 500 * 2 (заголовок + данные)
            $batchStartTime = microtime(true);
            
            try {
                $response = $client->bulk(['body' => $bulkData]);
                
                if (isset($response['errors']) && $response['errors']) {
                    foreach ($response['items'] as $item) {
                        if (isset($item['index']['error'])) {
                            error_log('Ошибка индексации товара ID ' . $item['index']['_id'] . ': ' . 
                                json_encode($item['index']['error']));
                            $errors++;
                        }
                    }
                }
                
                $batchTime = microtime(true) - $batchStartTime;
                $processed += count($bulkData) / 2;
                
                echo sprintf(
                    "Обработано: %d из %d (%.1f%%) | Время батча: %.2f сек | Ошибок: %d\n",
                    $processed,
                    $totalProducts,
                    ($processed / $totalProducts) * 100,
                    $batchTime,
                    $errors
                );
                
            } catch (\Exception $e) {
                error_log('Ошибка bulk индексации: ' . $e->getMessage());
                $errors += count($bulkData) / 2;
            }
            
            $bulkData = [];
        }
        
    } catch (\Exception $e) {
        error_log("Ошибка обработки товара ID $product_id: " . $e->getMessage());
        $errors++;
    }
}

// Отправляем оставшиеся данные
if (!empty($bulkData)) {
    try {
        $response = $client->bulk(['body' => $bulkData]);
        $processed += count($bulkData) / 2;
        
        if (isset($response['errors']) && $response['errors']) {
            foreach ($response['items'] as $item) {
                if (isset($item['index']['error'])) {
                    error_log('Ошибка финальной индексации: ' . json_encode($item['index']['error']));
                    $errors++;
                }
            }
        }
    } catch (\Exception $e) {
        error_log('Ошибка финальной индексации: ' . $e->getMessage());
        $errors += count($bulkData) / 2;
    }
}

// Обновляем индекс
echo "\nОбновление индекса...\n";
try {
    $client->indices()->refresh(['index' => 'products_v3']);
    echo "Индекс обновлен успешно\n";
} catch (\Exception $e) {
    echo "Ошибка обновления индекса: " . $e->getMessage() . "\n";
}

// Создаем алиас для плавного перехода
echo "\nСоздание алиаса products_current...\n";
try {
    // Удаляем старый алиас если есть
    $client->indices()->deleteAlias(['index' => '_all', 'name' => 'products_current']);
} catch (\Exception $e) {
    // Алиаса не было
}

try {
    $client->indices()->putAlias([
        'index' => 'products_v3',
        'name' => 'products_current'
    ]);
    echo "Алиас создан успешно\n";
} catch (\Exception $e) {
    echo "Ошибка создания алиаса: " . $e->getMessage() . "\n";
}

$totalTime = microtime(true) - $startTime;

echo "\n=====================================\n";
echo "=== ИНДЕКСАЦИЯ ЗАВЕРШЕНА ===\n";
echo "=====================================\n";
echo "Обработано товаров: $processed\n";
echo "Ошибок: $errors\n";
echo "Общее время: " . number_format($totalTime, 2) . " сек\n";
echo "Среднее время на товар: " . number_format($totalTime / $processed, 3) . " сек\n";
echo "\nДля использования нового индекса обновите get_protop.php\n";
echo "чтобы использовать индекс 'products_v3' или алиас 'products_current'\n";