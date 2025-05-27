<?php
/**
 * API умного поиска товаров v4
 * Безопасный, с разделением статических и динамических данных
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use OpenSearch\ClientBuilder;
use App\Services\DynamicProductDataService;
use App\Services\AuthService;
use App\Core\RateLimiter;
use App\Core\Validator;

// Настройки безопасности
header('Content-Type: application/json; charset=utf-8');
header('X-Content-Type-Options: nosniff');
header('X-Frame-Options: DENY');
header('X-XSS-Protection: 1; mode=block');

// CORS настройки (настройте под ваш домен!)
$allowedOrigins = ['https://vdestor.ru', 'http://localhost:3000'];
$origin = $_SERVER['HTTP_ORIGIN'] ?? '';

if (in_array($origin, $allowedOrigins)) {
    header("Access-Control-Allow-Origin: $origin");
    header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
    header('Access-Control-Allow-Headers: Content-Type, Authorization');
    header('Access-Control-Allow-Credentials: true');
}

// Обработка preflight запросов
if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(204);
    exit;
}

// Включаем обработку ошибок
set_error_handler(function($severity, $message, $file, $line) {
    throw new ErrorException($message, 0, $severity, $file, $line);
});

try {
    // Проверка rate limit
    session_start();
    $rateLimiter = new RateLimiter();
    $clientId = $_SESSION['user_id'] ?? $_SERVER['REMOTE_ADDR'];
    
    if (!$rateLimiter->check($clientId, 'search', 60, 30)) { // 30 запросов в минуту
        throw new \Exception('Слишком много запросов. Попробуйте позже.', 429);
    }
    
    // Валидация и санитизация входных данных
    $validator = new Validator();
    
    $params = [
        'q' => $validator->sanitizeString($_GET['q'] ?? ''),
        'page' => $validator->validateInt($_GET['page'] ?? 1, 1, 1000),
        'limit' => $validator->validateInt($_GET['limit'] ?? 20, 1, 100),
        'sort' => $validator->validateEnum($_GET['sort'] ?? 'relevance', 
            ['relevance', 'name', 'price_asc', 'price_desc', 'popularity']),
        'city_id' => $validator->validateInt($_GET['city_id'] ?? 1, 1, 10000),
        'filters' => []
    ];
    
    // Парсинг фильтров
    if (!empty($_GET['filters'])) {
        $filters = json_decode($_GET['filters'], true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new \Exception('Некорректный формат фильтров', 400);
        }
        
        // Валидация каждого фильтра
        $allowedFilters = ['brand', 'category', 'price_min', 'price_max', 'in_stock', 'attributes'];
        foreach ($filters as $key => $value) {
            if (!in_array($key, $allowedFilters)) {
                continue;
            }
            
            switch ($key) {
                case 'brand':
                case 'category':
                    $params['filters'][$key] = $validator->sanitizeString($value);
                    break;
                case 'price_min':
                case 'price_max':
                    $params['filters'][$key] = $validator->validateFloat($value, 0, 1000000);
                    break;
                case 'in_stock':
                    $params['filters'][$key] = (bool)$value;
                    break;
                case 'attributes':
                    if (is_array($value)) {
                        $params['filters'][$key] = array_map([$validator, 'sanitizeString'], $value);
                    }
                    break;
            }
        }
    }
    
    // Подключение к OpenSearch
    $client = ClientBuilder::create()
        ->setHosts(['localhost:9200'])
        ->build();
    
    // Строим запрос
    $searchBody = buildSearchQuery($params);
    
    // Выполняем поиск
    $response = $client->search([
        'index' => 'products_current',
        'body' => $searchBody
    ]);
    
    // Извлекаем ID найденных товаров
    $productIds = [];
    $products = [];
    
    foreach ($response['hits']['hits'] as $hit) {
        $product = $hit['_source'];
        $product['_score'] = $hit['_score'];
        $product['_highlight'] = $hit['highlight'] ?? [];
        
        $productIds[] = $product['product_id'];
        $products[$product['product_id']] = $product;
    }
    
    // Получаем динамические данные
    $dynamicData = [];
    if (!empty($productIds)) {
        $dynamicService = new DynamicProductDataService();
        $userId = AuthService::check() ? AuthService::user()['id'] : null;
        
        $dynamicData = $dynamicService->getProductsDynamicData(
            $productIds,
            $params['city_id'],
            $userId
        );
    }
    
    // Объединяем статические и динамические данные
    $results = [];
    foreach ($productIds as $productId) {
        $product = $products[$productId];
        $dynamic = $dynamicData[$productId] ?? [];
        
        // Добавляем динамические данные
        $product['price'] = $dynamic['price'] ?? null;
        $product['stock'] = $dynamic['stock'] ?? ['quantity' => 0];
        $product['delivery'] = $dynamic['delivery'] ?? ['text' => 'Уточняйте'];
        $product['available'] = $dynamic['available'] ?? false;
        
        $results[] = $product;
    }
    
    // Формируем ответ
    $output = [
        'success' => true,
        'data' => [
            'products' => $results,
            'total' => $response['hits']['total']['value'] ?? 0,
            'page' => $params['page'],
            'limit' => $params['limit'],
            'pages' => ceil(($response['hits']['total']['value'] ?? 0) / $params['limit'])
        ],
        'aggregations' => formatAggregations($response['aggregations'] ?? []),
        'query_info' => [
            'original' => $params['q'],
            'took' => $response['took'] ?? 0,
            'max_score' => $response['hits']['max_score'] ?? 0
        ]
    ];
    
    // Логирование успешного поиска (асинхронно)
    logSearch($params['q'], count($results), $params['city_id'], $userId ?? null);
    
    echo json_encode($output, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    
} catch (\Exception $e) {
    http_response_code($e->getCode() ?: 500);
    
    $error = [
        'success' => false,
        'error' => [
            'message' => $e->getMessage(),
            'code' => $e->getCode() ?: 500
        ]
    ];
    
    // В режиме разработки можно добавить trace
    if (getenv('APP_ENV') === 'development') {
        $error['error']['trace'] = $e->getTraceAsString();
    }
    
    echo json_encode($error, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
}

/**
 * Построение поискового запроса
 */
function buildSearchQuery(array $params): array
{
    $from = ($params['page'] - 1) * $params['limit'];
    
    $body = [
        'size' => $params['limit'],
        'from' => $from,
        'track_total_hits' => true,
        '_source' => [
            'product_id', 'external_id', 'sku', 'name', 'description',
            'brand_id', 'brand_name', 'series_id', 'series_name',
            'categories', 'category_ids', 'attributes',
            'unit', 'min_sale', 'weight', 'dimensions',
            'images', 'documents', 'created_at', 'updated_at'
        ]
    ];
    
    // Построение запроса
    $must = [];
    $filter = [];
    
    // Основной поисковый запрос
    if (!empty($params['q'])) {
        $query = $params['q'];
        
        // Определяем тип запроса
        $queryType = detectQueryType($query);
        
        switch ($queryType) {
            case 'code':
                $must[] = [
                    'bool' => [
                        'should' => [
                            ['term' => ['external_id.keyword' => strtolower($query)]],
                            ['term' => ['sku.keyword' => strtolower($query)]],
                            ['match' => ['external_id.autocomplete' => $query]],
                            ['match' => ['sku.autocomplete' => $query]]
                        ],
                        'minimum_should_match' => 1
                    ]
                ];
                break;
                
            case 'brand':
                $must[] = [
                    'multi_match' => [
                        'query' => $query,
                        'fields' => ['brand_name^10', 'name'],
                        'type' => 'best_fields',
                        'operator' => 'and'
                    ]
                ];
                break;
                
            default:
                $must[] = [
                    'multi_match' => [
                        'query' => $query,
                        'fields' => [
                            'name^10',
                            'name.autocomplete^5',
                            'description^3',
                            'brand_name^8',
                            'series_name^6',
                            'categories^4',
                            'external_id^7',
                            'sku^6'
                        ],
                        'type' => 'best_fields',
                        'operator' => 'or',
                        'minimum_should_match' => '70%',
                        'fuzziness' => 'AUTO'
                    ]
                ];
                
                // Boost для точных совпадений
                $body['rescore'] = [
                    'window_size' => 50,
                    'query' => [
                        'rescore_query' => [
                            'match_phrase' => [
                                'name' => [
                                    'query' => $query,
                                    'slop' => 2
                                ]
                            ]
                        ],
                        'query_weight' => 1.0,
                        'rescore_query_weight' => 2.0
                    ]
                ];
        }
        
        // Подсветка результатов
        $body['highlight'] = [
            'pre_tags' => ['<mark>'],
            'post_tags' => ['</mark>'],
            'fields' => [
                'name' => ['number_of_fragments' => 0],
                'description' => ['fragment_size' => 150, 'number_of_fragments' => 2],
                'brand_name' => ['number_of_fragments' => 0],
                'categories' => ['number_of_fragments' => 0]
            ]
        ];
    }
    
    // Фильтры
    if (!empty($params['filters']['brand'])) {
        $filter[] = ['term' => ['brand_name.keyword' => $params['filters']['brand']]];
    }
    
    if (!empty($params['filters']['category'])) {
        $filter[] = ['match' => ['categories' => $params['filters']['category']]];
    }
    
    // Фильтр атрибутов
    if (!empty($params['filters']['attributes'])) {
        foreach ($params['filters']['attributes'] as $attrName => $attrValue) {
            $filter[] = [
                'nested' => [
                    'path' => 'attributes',
                    'query' => [
                        'bool' => [
                            'must' => [
                                ['match' => ['attributes.name' => $attrName]],
                                ['match' => ['attributes.value' => $attrValue]]
                            ]
                        ]
                    ]
                ]
            ];
        }
    }
    
    // Формируем финальный запрос
    $body['query'] = ['bool' => []];
    
    if (!empty($must)) {
        $body['query']['bool']['must'] = $must;
    }
    
    if (!empty($filter)) {
        $body['query']['bool']['filter'] = $filter;
    }
    
    if (empty($must) && empty($filter)) {
        $body['query'] = ['match_all' => new \stdClass()];
    }
    
    // Сортировка
    switch ($params['sort']) {
        case 'name':
            $body['sort'] = [['name.keyword' => 'asc']];
            break;
        case 'popularity':
            // Сортировка по популярности требует отдельного поля
            $body['sort'] = [['_score' => 'desc'], ['name.keyword' => 'asc']];
            break;
        case 'relevance':
        default:
            if (!empty($params['q'])) {
                $body['sort'] = [['_score' => 'desc'], ['name.keyword' => 'asc']];
            } else {
                $body['sort'] = [['name.keyword' => 'asc']];
            }
    }
    
    // Агрегации для фильтров
    $body['aggs'] = [
        'brands' => [
            'terms' => [
                'field' => 'brand_name.keyword',
                'size' => 50,
                'order' => ['_count' => 'desc']
            ]
        ],
        'categories' => [
            'terms' => [
                'field' => 'categories.keyword',
                'size' => 30
            ]
        ],
        'attributes' => [
            'nested' => [
                'path' => 'attributes'
            ],
            'aggs' => [
                'names' => [
                    'terms' => [
                        'field' => 'attributes.name.keyword',
                        'size' => 20
                    ],
                    'aggs' => [
                        'values' => [
                            'terms' => [
                                'field' => 'attributes.value.keyword',
                                'size' => 10
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ];
    
    return $body;
}

/**
 * Определение типа запроса
 */
function detectQueryType(string $query): string
{
    $query = trim($query);
    
    // Проверка на код товара
    if (preg_match('/^[A-Za-z0-9\-\.\/\_\s]{2,30}$/u', $query)) {
        // Проверяем, не является ли это общим словом
        $commonWords = ['выключатель', 'розетка', 'кабель', 'лампа', 'автомат'];
        $queryLower = mb_strtolower($query);
        
        foreach ($commonWords as $word) {
            if (mb_strpos($queryLower, $word) !== false) {
                return 'text';
            }
        }
        
        // Если есть цифры и буквы - скорее всего код
        if (preg_match('/\d/', $query) && preg_match('/[A-Za-z]/', $query)) {
            return 'code';
        }
    }
    
    // Проверка на бренд
    $brands = ['schneider', 'legrand', 'abb', 'iek', 'ekf', 'dkc'];
    $queryLower = mb_strtolower($query);
    
    foreach ($brands as $brand) {
        if (mb_strpos($queryLower, $brand) !== false) {
            return 'brand';
        }
    }
    
    return 'text';
}

/**
 * Форматирование агрегаций
 */
function formatAggregations(array $aggregations): array
{
    $result = [];
    
    if (isset($aggregations['brands']['buckets'])) {
        $result['brands'] = array_map(function($bucket) {
            return [
                'name' => $bucket['key'],
                'count' => $bucket['doc_count']
            ];
        }, $aggregations['brands']['buckets']);
    }
    
    if (isset($aggregations['categories']['buckets'])) {
        $result['categories'] = array_map(function($bucket) {
            return [
                'name' => $bucket['key'],
                'count' => $bucket['doc_count']
            ];
        }, $aggregations['categories']['buckets']);
    }
    
    if (isset($aggregations['attributes']['names']['buckets'])) {
        $result['attributes'] = [];
        foreach ($aggregations['attributes']['names']['buckets'] as $nameBucket) {
            $values = [];
            if (isset($nameBucket['values']['buckets'])) {
                foreach ($nameBucket['values']['buckets'] as $valueBucket) {
                    $values[] = [
                        'value' => $valueBucket['key'],
                        'count' => $valueBucket['doc_count']
                    ];
                }
            }
            $result['attributes'][] = [
                'name' => $nameBucket['key'],
                'values' => $values
            ];
        }
    }
    
    return $result;
}

/**
 * Логирование поиска
 */
function logSearch(string $query, int $results, int $cityId, ?int $userId): void
{
    // Асинхронное логирование через очередь
    // TODO: Реализовать через RabbitMQ или Redis Queue
    
    // Временно - простое логирование в БД
    try {
        $pdo = \App\Core\Database::getConnection();
        $stmt = $pdo->prepare("
            INSERT INTO search_logs (query, results_count, city_id, user_id, created_at)
            VALUES (?, ?, ?, ?, NOW())
        ");
        $stmt->execute([$query, $results, $cityId, $userId]);
    } catch (\Exception $e) {
        error_log('Search logging failed: ' . $e->getMessage());
    }
}
