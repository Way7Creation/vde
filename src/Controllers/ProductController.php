<?php
namespace App\Controllers;

use App\Core\Database;
use App\Core\Layout;

class ProductController
{
    public function viewAction()
    {
        $id = $_GET['id'] ?? null;
        if (!$id) {
            http_response_code(404);
            Layout::render('errors/404', []);
            return;
        }
        $pdo = Database::getConnection();

        // 1. Основные данные
        $stmt = $pdo->prepare("
            SELECT p.*, b.name AS brand_name, s.name AS series_name
            FROM products p
            LEFT JOIN brands b ON b.brand_id = p.brand_id
            LEFT JOIN series s ON s.series_id = p.series_id
            WHERE p.product_id = :id OR p.external_id = :id
            LIMIT 1
        ");
        $stmt->execute(['id' => $id]);
        $product = $stmt->fetch();
        if (!$product) {
            http_response_code(404);
            Layout::render('errors/404', []);
            return;
        }

        // 2. Изображения (галерея)
        $imgStmt = $pdo->prepare("SELECT url, alt_text FROM product_images WHERE product_id = :pid ORDER BY is_main DESC, sort_order ASC");
        $imgStmt->execute(['pid' => $product['product_id']]);
        $images = $imgStmt->fetchAll(\PDO::FETCH_ASSOC);

        // 3. Документы/сертификаты (sort_order нет, сортируем по document_id)
        $docStmt = $pdo->prepare("SELECT * FROM product_documents WHERE product_id = :pid ORDER BY document_id ASC");
        $docStmt->execute(['pid' => $product['product_id']]);
        $documents = $docStmt->fetchAll(\PDO::FETCH_ASSOC);

        // 4. Характеристики
        $attrStmt = $pdo->prepare("SELECT name, value FROM product_attributes WHERE product_id = :pid ORDER BY sort_order ASC");
        $attrStmt->execute(['pid' => $product['product_id']]);
        $attributes = $attrStmt->fetchAll(\PDO::FETCH_ASSOC);

        // 5. Базовая цена
        $priceStmt = $pdo->prepare("SELECT price FROM prices WHERE product_id = :pid AND is_base=1 ORDER BY valid_from DESC LIMIT 1");
        $priceStmt->execute(['pid' => $product['product_id']]);
        $price = $priceStmt->fetchColumn();

        // 6. Остатки (наличие, fallback)
        $stockStmt = $pdo->prepare("SELECT SUM(quantity - reserved) FROM stock_balances WHERE product_id = :pid");
        $stockStmt->execute(['pid' => $product['product_id']]);
        $stock = $stockStmt->fetchColumn();

        // 7. Связанные товары (upsell, cross-sell)
        // Исправленный запрос с учетом структуры related_products (используем related_id вместо related_product_id)
        $relStmt = $pdo->prepare(
            "SELECT p.name, p.external_id 
             FROM related_products rp 
             JOIN products p ON p.product_id = rp.related_id 
             WHERE rp.product_id = :pid"
        );
        $relStmt->execute(['pid' => $product['product_id']]);
        $related = $relStmt->fetchAll(\PDO::FETCH_ASSOC);

        Layout::render('shop/product', [
            'product'    => $product,
            'images'     => $images,
            'documents'  => $documents,
            'attributes' => $attributes,
            'price'      => $price,
            'stock'      => $stock,
            'related'    => $related,
        ]);
    }
    public function catalogAction(): void
    {
        $client = \OpenSearch\ClientBuilder::create()->build();
        
        $page = (int)($_GET['page'] ?? 1);
        $perPage = 20;
        $from = ($page - 1) * $perPage;
        
        $params = [
            'index' => 'products_current',
            'body' => [
                'from' => $from,
                'size' => $perPage,
                'query' => ['match_all' => (object)[]],
                'sort' => ['name.keyword' => 'asc']
            ]
        ];
        
        if (isset($_GET['category'])) {
            $params['body']['query'] = [
                'term' => ['categories' => $_GET['category']]
            ];
        }
        
        $response = $client->search($params);
        
        $products = [];
        foreach ($response['hits']['hits'] as $hit) {
            $products[] = $hit['_source'];
        }
        
        $totalProducts = $response['hits']['total']['value'] ?? 0;
        $totalPages = ceil($totalProducts / $perPage);
        
        Layout::render('shop/catalog', [
            'products' => $products,
            'currentPage' => $page,
            'totalPages' => $totalPages,
            'totalProducts' => $totalProducts
        ]);
    }
    
    /**
     * Ajax endpoint для получения товаров в JSON
     */
    public function ajaxProductsAction(): void
    {
        header('Content-Type: application/json; charset=utf-8');
        
        $client = \OpenSearch\ClientBuilder::create()->build();
        
        $page = (int)($_GET['page'] ?? 1);
        $perPage = (int)($_GET['itemsPerPage'] ?? 20);
        $from = ($page - 1) * $perPage;
        
        $query = ['bool' => ['must' => []]];
        
        // Поиск
        if (!empty($_GET['search'])) {
            $query['bool']['must'][] = [
                'multi_match' => [
                    'query' => $_GET['search'],
                    'fields' => ['name^3', 'sku^2', 'external_id', 'brand_name', 'series_name'],
                    'type' => 'best_fields',
                    'operator' => 'or'
                ]
            ];
        }
        
        // Фильтры
        if (!empty($_GET['brand_name'])) {
            $query['bool']['must'][] = ['term' => ['brand_name' => $_GET['brand_name']]];
        }
        
        if (!empty($_GET['category'])) {
            $query['bool']['must'][] = ['term' => ['categories' => $_GET['category']]];
        }
        
        // Если нет условий, используем match_all
        if (empty($query['bool']['must'])) {
            $query = ['match_all' => (object)[]];
        }
        
        // Сортировка
        $sortField = $_GET['sortColumn'] ?? 'name';
        $sortDir = $_GET['sortDirection'] ?? 'asc';
        
        $params = [
            'index' => 'products_current',
            'body' => [
                'from' => $from,
                'size' => $perPage,
                'query' => $query,
                'sort' => [[$sortField . '.keyword' => ['order' => $sortDir]]]
            ]
        ];
        
        try {
            $response = $client->search($params);
            
            $products = [];
            foreach ($response['hits']['hits'] as $hit) {
                $products[] = $hit['_source'];
            }
            
            echo json_encode([
                'products' => $products,
                'totalProducts' => $response['hits']['total']['value'] ?? 0,
                'page' => $page,
                'itemsPerPage' => $perPage
            ]);
        } catch (\Exception $e) {
            http_response_code(500);
            echo json_encode(['error' => 'Ошибка поиска']);
        }
    }
    
    public function searchAction(): void
    {
        $query = trim($_GET['q'] ?? '');
        
        if (empty($query)) {
            Layout::render('shop/search', ['products' => [], 'query' => '']);
            return;
        }
        
        $client = \OpenSearch\ClientBuilder::create()->build();
        
        $params = [
            'index' => 'products_current',
            'body' => [
                'size' => 50,
                'query' => [
                    'multi_match' => [
                        'query' => $query,
                        'fields' => ['search_text^3', 'name^2', 'description', 'sku'],
                        'type' => 'best_fields',
                        'operator' => 'or',
                        'fuzziness' => 'AUTO'
                    ]
                ],
                'highlight' => [
                    'fields' => [
                        'name' => (object)[],
                        'description' => ['fragment_size' => 150]
                    ]
                ]
            ]
        ];
        
        $response = $client->search($params);
        
        $products = [];
        foreach ($response['hits']['hits'] as $hit) {
            $product = $hit['_source'];
            $product['_highlight'] = $hit['highlight'] ?? [];
            $products[] = $product;
        }
        
        Layout::render('shop/search', [
            'products' => $products,
            'query' => $query,
            'total' => $response['hits']['total']['value'] ?? 0
        ]);
    }
}