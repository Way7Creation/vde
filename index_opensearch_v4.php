<?php
/**
 * Скрипт индексации товаров в OpenSearch v4
 * Оптимизированная версия для больших объемов данных (600k+ записей)
 */

require __DIR__ . '/vendor/autoload.php';

use OpenSearch\ClientBuilder;
use PDO;

// Конфигурация
const BATCH_SIZE = 1000;
const MEMORY_LIMIT = '60G';
const MAX_EXECUTION_TIME = 3600;

class Indexer {
    private $client;
    private $pdo;
    private $processed = 0;
    private $errors = 0;
    private $startTime;

    public function __construct() {
        $this->startTime = microtime(true);
        $this->initializeOpenSearch();
        $this->initializeDatabase();
    }

    private function initializeOpenSearch(): void {
        $this->client = ClientBuilder::create()
            ->setHosts(['localhost:9200'])
            ->build();
    }

    private function initializeDatabase(): void {
        $config = parse_ini_file('/var/www/www-root/data/config/config_bd.ini', true)['mysql'];
        $dsn = "mysql:host={$config['host']};dbname={$config['database']};charset=utf8mb4";
        
        $this->pdo = new PDO($dsn, $config['user'], $config['password'], [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true,
            PDO::MYSQL_ATTR_INIT_COMMAND => "SET SESSION sql_mode='STRICT_ALL_TABLES'"
        ]);
    }

    public function run(): void {
        try {
            $this->prepareIndex();
            $this->processProducts();
            $this->finalizeIndex();
        } catch (Throwable $e) {
            $this->handleCriticalError($e);
        }
    }

    private function prepareIndex(): void {
        echo "=== Индексация товаров OpenSearch v4 ===\n\n";
        echo "OpenSearch версия: " . $this->client->info()['version']['number'] . "\n\n";

        $this->deleteOldIndex();
        $this->createNewIndex();
    }

    private function deleteOldIndex(): void {
        echo "Удаление старого индекса...\n";
        try {
            $this->client->indices()->delete(['index' => 'products_current']);
            echo "Индекс удален\n";
        } catch (Exception) {
            echo "Индекс не существует\n";
        }
    }

    private function createNewIndex(): void {
        echo "Создание нового индекса...\n";
        $indexConfig = json_decode(file_get_contents(__DIR__ . '/products_v4.json'), true);
        $this->client->indices()->create([
            'index' => 'products_current',
            'body' => $indexConfig
        ]);
        echo "Индекс создан успешно\n\n";
    }

    private function processProducts(): void {
        $totalCount = $this->getTotalProductsCount();
        echo "Найдено товаров для индексации: $totalCount\n\n";

        $page = 1;
        do {
            $products = $this->fetchProductsBatch($page);
            if (empty($products)) break;

            $this->processBatch($products, $totalCount);
            $page++;
            
            gc_collect_cycles();
            unset($products);
        } while (true);
    }

    private function getTotalProductsCount(): int {
        return (int)$this->pdo->query("SELECT COUNT(*) FROM products")->fetchColumn();
    }

    private function fetchProductsBatch(int $page): array {
        $offset = ($page - 1) * BATCH_SIZE;
        
        $stmt = $this->pdo->prepare("
            SELECT p.*, 
                   b.name AS brand_name,
                   s.name AS series_name,
                   GROUP_CONCAT(DISTINCT c.category_id) AS category_ids,
                   GROUP_CONCAT(DISTINCT c.name SEPARATOR '|') AS categories
            FROM products p
            LEFT JOIN brands b ON p.brand_id = b.brand_id
            LEFT JOIN series s ON p.series_id = s.series_id
            LEFT JOIN product_categories pc ON p.product_id = pc.product_id
            LEFT JOIN categories c ON pc.category_id = c.category_id
            GROUP BY p.product_id
            ORDER BY p.product_id
            LIMIT :limit OFFSET :offset
        ");
        
        $stmt->bindValue(':limit', BATCH_SIZE, PDO::PARAM_INT);
        $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        $stmt->execute();
        
        return $stmt->fetchAll();
    }

    private function processBatch(array $products, int $totalCount): void {
        $bulkData = [];
        
        foreach ($products as $product) {
            try {
                $this->normalizeProductData($product);
                $this->enrichProductData($product);
                $this->prepareBulkData($bulkData, $product);
            } catch (Exception $e) {
                $this->logProductError($product['product_id'], $e);
            }
        }
        
        $this->sendBulkRequest($bulkData);
        $this->printProgress($totalCount);
    }

    private function normalizeProductData(array &$product): void {
        $fields = ['external_id', 'sku', 'name', 'description', 'brand_name', 'series_name'];
        foreach ($fields as $field) {
            $product[$field] = $this->normalizeText($product[$field] ?? '');
        }
        
        if (isset($product['categories'])) {
            $product['categories'] = explode('|', $product['categories']);
            $product['category_ids'] = array_map('intval', explode(',', $product['category_ids']));
        }
    }

    private function normalizeText(?string $str): string {
        if (empty($str)) return '';
        
        $str = preg_replace('/[^\P{C}\t\n\r]+/u', '', $str);
        $str = preg_replace('/\s+/', ' ', $str);
        return trim($str);
    }

    private function enrichProductData(array &$product): void {
        $product['attributes'] = $this->fetchAttributes($product['product_id']);
        $product['images'] = $this->fetchImages($product['product_id']);
        $product['documents'] = $this->fetchDocuments($product['product_id']);
        $product['suggest'] = $this->createSuggestData($product);
        
        $product['created_at'] = date('c', strtotime($product['created_at']));
        $product['updated_at'] = date('c', strtotime($product['updated_at']));
    }

    private function fetchAttributes(int $productId): array {
        $stmt = $this->pdo->prepare("
            SELECT name, value, unit 
            FROM product_attributes 
            WHERE product_id = ?
            ORDER BY sort_order
        ");
        $stmt->execute([$productId]);
        
        return array_map(function($attr) {
            return [
                'name' => $this->normalizeText($attr['name']),
                'value' => $this->normalizeText($attr['value']),
                'unit' => $attr['unit'] ?? ''
            ];
        }, $stmt->fetchAll());
    }

    private function fetchImages(int $productId): array {
        $stmt = $this->pdo->prepare("
            SELECT url 
            FROM product_images
            WHERE product_id = ?
            ORDER BY is_main DESC, sort_order
        ");
        $stmt->execute([$productId]);
        return $stmt->fetchAll(PDO::FETCH_COLUMN, 0);
    }

    private function fetchDocuments(int $productId): array {
        $stmt = $this->pdo->prepare("
            SELECT type, COUNT(*) as count
            FROM product_documents
            WHERE product_id = ?
            GROUP BY type
        ");
        $stmt->execute([$productId]);
        
        $docs = [];
        foreach ($stmt->fetchAll(PDO::FETCH_KEY_PAIR) as $type => $count) {
            $docs[$type.'s'] = (int)$count;
        }
        return $docs;
    }

    private function createSuggestData(array $product): array {
        $suggestions = [];
        
        if (!empty($product['name'])) {
            $suggestions[] = [
                'input' => [$product['name']],
                'weight' => 100
            ];
            
            $words = explode(' ', $product['name']);
            if (count($words) > 2) {
                $suggestions[] = [
                    'input' => [implode(' ', array_slice($words, 0, 2))],
                    'weight' => 80
                ];
            }
        }
        
        if (!empty($product['external_id'])) {
            $suggestions[] = [
                'input' => [
                    $product['external_id'],
                    str_replace(['-', '_', '/', ' '], '', $product['external_id'])
                ],
                'weight' => 90
            ];
        }
        
        if (!empty($product['brand_name'])) {
            $suggestions[] = [
                'input' => [$product['brand_name']],
                'weight' => 70
            ];
        }
        
        return $suggestions;
    }

    private function prepareBulkData(array &$bulkData, array $product): void {
        $bulkData[] = ['index' => ['_index' => 'products_current', '_id' => $product['product_id']]];
        $bulkData[] = $product;
    }

    private function sendBulkRequest(array $bulkData): void {
        if (!empty($bulkData)) {
            $response = $this->client->bulk(['body' => $bulkData]);
            $this->processed += count($bulkData)/2;
            
            if ($response['errors'] ?? false) {
                foreach ($response['items'] as $item) {
                    if ($error = $item['index']['error'] ?? null) {
                        $this->errors++;
                        error_log('Ошибка индексации: '.json_encode($error));
                    }
                }
            }
        }
    }

    private function printProgress(int $totalCount): void {
        $progress = round(($this->processed / $totalCount) * 100, 1);
        $memory = round(memory_get_usage(true)/1024/1024, 2).'MB';
        $time = number_format(microtime(true) - $this->startTime, 1);
        
        echo "\rПрогресс: $progress% | Обработано: {$this->processed} | Ошибки: {$this->errors} | Память: $memory | Время: {$time}s ";
    }

    private function finalizeIndex(): void {
        $this->client->indices()->refresh(['index' => 'products_current']);
        
        echo "\n\nСоздание алиаса...\n";
        try {
            $this->client->indices()->deleteAlias(['index' => '_all', 'name' => 'products_current']);
        } catch (Exception) {}
        
        $this->client->indices()->putAlias([
            'index' => 'products_current',
            'name' => 'products_current'
        ]);
        
        $this->printFinalStats();
    }

    private function printFinalStats(): void {
        $totalTime = microtime(true) - $this->startTime;
        
        echo "\n\n=== ИНДЕКСАЦИЯ ЗАВЕРШЕНА ===\n";
        echo "Обработано: {$this->processed} товаров\n";
        echo "Ошибок: {$this->errors}\n";
        echo "Общее время: ".number_format($totalTime, 1)." сек\n";
        echo "Скорость: ".number_format($this->processed/$totalTime, 0)." записей/сек\n";
    }

    private function handleCriticalError(Throwable $e): void {
        echo "\n\nКРИТИЧЕСКАЯ ОШИБКА: ".$e->getMessage()."\n";
        echo "Trace: ".$e->getTraceAsString()."\n";
        exit(1);
    }
}

// Запуск скрипта
ini_set('memory_limit', MEMORY_LIMIT);
ini_set('max_execution_time', MAX_EXECUTION_TIME);
set_time_limit(0);

(new Indexer())->run();