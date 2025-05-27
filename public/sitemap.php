<?php
header('Content-Type: application/xml; charset=utf-8');

require_once __DIR__ . '/../vendor/autoload.php';
use App\Core\Database;

$baseUrl = 'https://vdestor.ru';

echo '<?xml version="1.0" encoding="UTF-8"?>';
?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <!-- Главная страница -->
    <url>
        <loc><?= $baseUrl ?>/</loc>
        <changefreq>daily</changefreq>
        <priority>1.0</priority>
    </url>
    
    <!-- Каталог -->
    <url>
        <loc><?= $baseUrl ?>/shop</loc>
        <changefreq>daily</changefreq>
        <priority>0.9</priority>
    </url>
    
    <!-- Категории -->
    <?php
    $pdo = Database::getConnection();
    $categories = $pdo->query("SELECT slug FROM categories WHERE slug IS NOT NULL")->fetchAll();
    foreach ($categories as $cat): ?>
    <url>
        <loc><?= $baseUrl ?>/shop/category/<?= htmlspecialchars($cat['slug']) ?></loc>
        <changefreq>weekly</changefreq>
        <priority>0.8</priority>
    </url>
    <?php endforeach; ?>
    
    <!-- Товары (первые 1000 популярных) -->
    <?php
    $products = $pdo->query("
        SELECT external_id, updated_at 
        FROM products 
        WHERE external_id IS NOT NULL 
        ORDER BY product_id DESC 
        LIMIT 1000
    ")->fetchAll();
    foreach ($products as $product): ?>
    <url>
        <loc><?= $baseUrl ?>/shop/product?id=<?= urlencode($product['external_id']) ?></loc>
        <lastmod><?= date('c', strtotime($product['updated_at'])) ?></lastmod>
        <changefreq>monthly</changefreq>
        <priority>0.7</priority>
    </url>
    <?php endforeach; ?>
</urlset>
