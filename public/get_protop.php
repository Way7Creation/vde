<?php
/**
 * Умный поиск товаров v3
 * Интеллектуальная обработка запросов с автоматическим определением типа
 */

header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

ini_set('display_errors', 0);
ini_set('log_errors', 1);
error_reporting(E_ALL);

require __DIR__ . '/../vendor/autoload.php';
use OpenSearch\ClientBuilder;

$client = ClientBuilder::create()
    ->setHosts(['localhost:9200'])
    ->build();

$page = max(1, intval($_GET['page'] ?? 1));
$limit = max(1, intval($_GET['itemsPerPage'] ?? 20));
$offset = ($page - 1) * $limit;

// Функция определения типа запроса
function detectQueryType($query) {
    $query = trim($query);
    
    // Код товара - только буквы, цифры и разделители
    if (preg_match('/^[A-Za-z0-9\-\.\/\_\s]+$/u', $query) && strlen($query) <= 30) {
        // Проверяем, не является ли это общим словом
        $commonWords = ['выключатель', 'розетка', 'кабель', 'лампа', 'автомат'];
        $queryLower = mb_strtolower($query);
        foreach ($commonWords as $word) {
            if (strpos($queryLower, $word) !== false) {
                return 'text';
            }
        }
        return 'code';
    }
    
    // Числовой запрос с единицами измерения
    if (preg_match('/^\d+[\s\-]*(вт|ватт|квт|а|ампер|в|вольт|мм|см|м|mm|cm|m|w|a|v)?$/iu', $query)) {
        return 'numeric';
    }
    
    // Проверка на бренд
    $brands = [
        'schneider', 'шнайдер', 'шнейдер',
        'legrand', 'легранд', 
        'abb', 'абб',
        'iek', 'иэк', 'иек',
        'ekf', 'экф', 'екф',
        'dkc', 'дкс',
        'кэаз', 'keaz',
        'контактор', 'kontar'
    ];
    
    $queryLower = mb_strtolower($query);
    foreach ($brands as $brand) {
        if (strpos($queryLower, $brand) !== false) {
            return 'brand';
        }
    }
    
    // Категория товара
    $categories = [
        'выключатель', 'розетка', 'кабель', 'провод', 
        'лампа', 'светильник', 'автомат', 'щит', 
        'удлинитель', 'трансформатор', 'стабилизатор'
    ];
    
    foreach ($categories as $category) {
        if (strpos($queryLower, $category) !== false) {
            return 'category';
        }
    }
    
    return 'text';
}

// Основной запрос
$body = [
    'size' => $limit,
    'from' => $offset,
    'track_total_hits' => true,
    '_source' => [
        'product_id', 'external_id', 'sku', 'name', 'description',
        'brand_name', 'series_name', 'categories', 'image_urls',
        'unit', 'min_sale', 'base_price', 'retail_price', 
        'stock_total', 'in_stock', 'attributes'
    ],
    'highlight' => [
        'pre_tags' => ['<mark>'],
        'post_tags' => ['</mark>'],
        'fields' => [
            'name' => [
                'fragment_size' => 200,
                'number_of_fragments' => 1
            ],
            'description' => [
                'fragment_size' => 150,
                'number_of_fragments' => 2
            ],
            'external_id' => ['number_of_fragments' => 1],
            'sku' => ['number_of_fragments' => 1],
            'brand_name' => ['number_of_fragments' => 1]
        ]
    ]
];

// Обработка фильтров
$filters = [];
if (!empty($_GET['filters'])) {
    $tmp = json_decode($_GET['filters'], true);
    if (json_last_error() === JSON_ERROR_NONE && is_array($tmp)) {
        $filters = $tmp;
    }
}

// Построение запроса
$mustClauses = [];
$shouldClauses = [];
$filterClauses = [];

// Поисковый запрос
if (!empty($filters['search'])) {
    $searchQuery = trim($filters['search']);
    $queryType = detectQueryType($searchQuery);
    
    error_log("Search query: '$searchQuery', type: $queryType");
    
    switch ($queryType) {
        case 'code':
            // Поиск по коду товара
            $cleanCode = strtolower(preg_replace('/[\s\-_\/]+/', '', $searchQuery));
            
            $shouldClauses = [
                // Точное совпадение - максимальный приоритет
                [
                    'term' => [
                        'external_id.keyword' => [
                            'value' => strtolower($searchQuery),
                            'boost' => 1000
                        ]
                    ]
                ],
                [
                    'term' => [
                        'sku.keyword' => [
                            'value' => strtolower($searchQuery),
                            'boost' => 900
                        ]
                    ]
                ],
                // Без разделителей
                [
                    'term' => [
                        'external_id.keyword' => [
                            'value' => $cleanCode,
                            'boost' => 800
                        ]
                    ]
                ],
                // Префиксный поиск
                [
                    'prefix' => [
                        'external_id.keyword' => [
                            'value' => strtolower($searchQuery),
                            'boost' => 500
                        ]
                    ]
                ],
                [
                    'prefix' => [
                        'sku.keyword' => [
                            'value' => strtolower($searchQuery),
                            'boost' => 400
                        ]
                    ]
                ],
                // N-gram поиск для частичного совпадения
                [
                    'match' => [
                        'external_id.ngram' => [
                            'query' => $searchQuery,
                            'boost' => 100
                        ]
                    ]
                ],
                // Поиск в названии
                [
                    'match_phrase' => [
                        'name' => [
                            'query' => $searchQuery,
                            'boost' => 50,
                            'slop' => 1
                        ]
                    ]
                ]
            ];
            
            $body['query'] = [
                'bool' => [
                    'should' => $shouldClauses,
                    'minimum_should_match' => 1
                ]
            ];
            break;
            
        case 'numeric':
            // Поиск по числовым характеристикам
            $mustClauses[] = [
                'nested' => [
                    'path' => 'attributes',
                    'query' => [
                        'bool' => [
                            'must' => [
                                'match' => [
                                    'attributes.value' => [
                                        'query' => $searchQuery,
                                        'operator' => 'and'
                                    ]
                                ]
                            ]
                        ]
                    ],
                    'inner_hits' => [
                        'size' => 5,
                        'highlight' => [
                            'fields' => [
                                'attributes.value' => {}
                            ]
                        ]
                    ]
                ]
            ];
            break;
            
        case 'brand':
            // Поиск по бренду
            $mustClauses[] = [
                'multi_match' => [
                    'query' => $searchQuery,
                    'fields' => [
                        'brand_name^10',
                        'brand_name.ngram^5',
                        'name^3',
                        'search_text'
                    ],
                    'type' => 'best_fields',
                    'operator' => 'and',
                    'fuzziness' => 'AUTO'
                ]
            ];
            break;
            
        case 'category':
            // Поиск по категории
            $mustClauses[] = [
                'multi_match' => [
                    'query' => $searchQuery,
                    'fields' => [
                        'categories^10',
                        'name^5',
                        'search_text^2'
                    ],
                    'type' => 'best_fields',
                    'operator' => 'and'
                ]
            ];
            break;
            
        default:
            // Общий текстовый поиск
            $mustClauses[] = [
                'bool' => [
                    'should' => [
                        // Точная фраза
                        [
                            'match_phrase' => [
                                'name' => [
                                    'query' => $searchQuery,
                                    'boost' => 100,
                                    'slop' => 2
                                ]
                            ]
                        ],
                        // Все слова в названии
                        [
                            'match' => [
                                'name' => [
                                    'query' => $searchQuery,
                                    'operator' => 'and',
                                    'boost' => 50
                                ]
                            ]
                        ],
                        // Частичное совпадение в названии
                        [
                            'match' => [
                                'name' => [
                                    'query' => $searchQuery,
                                    'operator' => 'or',
                                    'minimum_should_match' => '75%',
                                    'boost' => 30
                                ]
                            ]
                        ],
                        // Автодополнение
                        [
                            'match' => [
                                'name.autocomplete' => [
                                    'query' => $searchQuery,
                                    'boost' => 25
                                ]
                            ]
                        ],
                        // Поиск с опечатками
                        [
                            'match' => [
                                'name' => [
                                    'query' => $searchQuery,
                                    'fuzziness' => 'AUTO',
                                    'prefix_length' => 2,
                                    'boost' => 20
                                ]
                            ]
                        ],
                        // N-gram поиск
                        [
                            'match' => [
                                'name.ngram' => [
                                    'query' => $searchQuery,
                                    'boost' => 15
                                ]
                            ]
                        ],
                        // Поиск в других полях
                        [
                            'multi_match' => [
                                'query' => $searchQuery,
                                'fields' => [
                                    'description^5',
                                    'brand_name^8',
                                    'series_name^6',
                                    'categories^4',
                                    'search_text^2'
                                ],
                                'type' => 'best_fields',
                                'operator' => 'or',
                                'minimum_should_match' => '50%',
                                'fuzziness' => 'AUTO'
                            ]
                        ]
                    ],
                    'minimum_should_match' => 1
                ]
            ];
            
            // Добавляем поиск в атрибутах
            $shouldClauses[] = [
                'nested' => [
                    'path' => 'attributes',
                    'query' => [
                        'multi_match' => [
                            'query' => $searchQuery,
                            'fields' => ['attributes.name', 'attributes.value'],
                            'type' => 'best_fields'
                        ]
                    ],
                    'boost' => 5
                ]
            ];
    }
}

if (!empty($products)) {
    // Получаем ID найденных товаров
    $productIds = array_column($products, 'product_id');
    
    // Загружаем динамические данные
    require_once __DIR__ . '/../src/Services/DynamicProductDataService.php';
    $dynamicService = new \App\Services\DynamicProductDataService();
    
    // Определяем пользователя
    session_start();
    $userId = $_SESSION['user_id'] ?? null;
    $cityId = (int)($_GET['city_id'] ?? 1);
    
    // Получаем цены и остатки
    $dynamicData = $dynamicService->getProductsDynamicData($productIds, $cityId, $userId);
    
    // Обогащаем товары динамическими данными
    foreach ($products as &$product) {
        $pid = $product['product_id'];
        if (isset($dynamicData[$pid])) {
            // Добавляем актуальную цену
            $product['base_price'] = $dynamicData[$pid]['price']['final'] ?? null;
            $product['original_price'] = $dynamicData[$pid]['price']['base'] ?? null;
            $product['has_special_price'] = $dynamicData[$pid]['price']['has_special'] ?? false;
            
            // Добавляем остатки
            $product['in_stock'] = $dynamicData[$pid]['available'] ?? false;
            $product['stock_quantity'] = $dynamicData[$pid]['stock']['quantity'] ?? 0;
            
            // Добавляем информацию о доставке
            $product['delivery_date'] = $dynamicData[$pid]['delivery']['date'] ?? null;
            $product['delivery_text'] = $dynamicData[$pid]['delivery']['text'] ?? 'Уточняйте';
        }
    }
}

// Фильтры по точным значениям
if (!empty($filters['brand_name'])) {
    $filterClauses[] = ['term' => ['brand_name.keyword' => $filters['brand_name']]];
}

if (!empty($filters['series_name'])) {
    $filterClauses[] = ['term' => ['series_name.keyword' => $filters['series_name']]];
}

if (!empty($filters['category'])) {
    $filterClauses[] = ['match' => ['categories' => $filters['category']]];
}

// Фильтр по цене
if (!empty($filters['price_min']) || !empty($filters['price_max'])) {
    $priceFilter = ['range' => ['base_price' => []]];
    if (!empty($filters['price_min'])) {
        $priceFilter['range']['base_price']['gte'] = (float)$filters['price_min'];
    }
    if (!empty($filters['price_max'])) {
        $priceFilter['range']['base_price']['lte'] = (float)$filters['price_max'];
    }
    $filterClauses[] = $priceFilter;
}

// Фильтр по наличию
if (isset($filters['in_stock'])) {
    $filterClauses[] = ['term' => ['in_stock' => (bool)$filters['in_stock']]];
}

// Формируем финальный запрос
if (!empty($mustClauses) || !empty($shouldClauses) || !empty($filterClauses)) {
    $body['query'] = ['bool' => []];
    
    if (!empty($mustClauses)) {
        $body['query']['bool']['must'] = $mustClauses;
    }
    
    if (!empty($shouldClauses)) {
        $body['query']['bool']['should'] = $shouldClauses;
    }
    
    if (!empty($filterClauses)) {
        $body['query']['bool']['filter'] = $filterClauses;
    }
} else {
    $body['query'] = ['match_all' => new \stdClass()];
}

// Сортировка
$sortColumn = $_GET['sortColumn'] ?? 'name';
$sortDir = strtolower($_GET['sortDirection'] ?? 'asc');

// Маппинг полей для сортировки
$sortFieldMap = [
    'name' => 'name.keyword',
    'external_id' => 'external_id.keyword',
    'sku' => 'sku.keyword',
    'brand_name' => 'brand_name.keyword',
    'series_name' => 'series_name.keyword',
    'base_price' => 'base_price',
    'retail_price' => 'retail_price',
    'stock_total' => 'stock_total',
    'created_at' => 'created_at',
    'updated_at' => 'updated_at'
];

$sortField = $sortFieldMap[$sortColumn] ?? 'name.keyword';

// Специальная сортировка для поиска
if (!empty($filters['search'])) {
    $body['sort'] = [
        '_score' => ['order' => 'desc'],
        $sortField => ['order' => $sortDir]
    ];
} else {
    $body['sort'] = [
        $sortField => ['order' => $sortDir]
    ];
}

// Агрегации для фильтров
$body['aggs'] = [
    'brands' => [
        'terms' => [
            'field' => 'brand_name.keyword',
            'size' => 100,
            'order' => ['_key' => 'asc']
        ]
    ],
    'series' => [
        'terms' => [
            'field' => 'series_name.keyword',
            'size' => 100,
            'order' => ['_key' => 'asc']
        ]
    ],
    'categories' => [
        'terms' => [
            'field' => 'categories.keyword',
            'size' => 100,
            'order' => ['_count' => 'desc']
        ]
    ],
    'price_stats' => [
        'stats' => [
            'field' => 'base_price'
        ]
    ],
    'in_stock_count' => [
        'filter' => [
            'term' => ['in_stock' => true]
        ]
    ]
];

// Выполнение запроса
try {
    error_log('OpenSearch query: ' . json_encode($body, JSON_UNESCAPED_UNICODE));
    
    $response = $client->search([
        'index' => 'products_current',
        'body' => $body
    ]);
    
    $products = [];
    
    foreach ($response['hits']['hits'] as $hit) {
        $product = $hit['_source'];
        $product['_score'] = $hit['_score'] ?? 0;
        
        // Добавляем подсветку
        if (isset($hit['highlight'])) {
            $product['_highlight'] = $hit['highlight'];
        }
        
        // Добавляем inner_hits для атрибутов
        if (isset($hit['inner_hits'])) {
            $product['_inner_hits'] = $hit['inner_hits'];
        }
        
        $products[] = $product;
    }
    
    // Формируем ответ
    $result = [
        'products' => $products,
        'totalProducts' => $response['hits']['total']['value'] ?? 0,
        'page' => $page,
        'limit' => $limit,
        'aggregations' => $response['aggregations'] ?? [],
        'query_info' => [
            'type' => $queryType ?? 'none',
            'query' => $searchQuery ?? '',
            'took' => $response['took'] ?? 0
        ]
    ];
    
    echo json_encode($result, JSON_UNESCAPED_UNICODE | JSON_NUMERIC_CHECK);
    
} catch (\Exception $e) {
    error_log('OpenSearch error: ' . $e->getMessage());
    error_log('Stack trace: ' . $e->getTraceAsString());
    
    http_response_code(500);
    echo json_encode([
        'error' => 'Ошибка поиска',
        'message' => $e->getMessage(),
        'products' => [],
        'totalProducts' => 0
    ], JSON_UNESCAPED_UNICODE);
}