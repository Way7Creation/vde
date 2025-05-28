<?php
declare(strict_types=1);

// Загружаем autoloader
require_once __DIR__ . '/../vendor/autoload.php';

// Инициализируем систему
use App\Core\Config;
use App\Core\SecurityManager;
use App\Core\Logger;
use App\Core\Session;
use App\Core\Router;

try {
    // Инициализируем конфигурацию
    Config::get('app.name'); // Вызываем для загрузки

    // Проверяем безопасность конфигурации
    $securityIssues = Config::validateSecurity();
    if (!empty($securityIssues)) {
        Logger::critical("Configuration security issues detected", $securityIssues);
    }

    // Инициализируем систему безопасности
    SecurityManager::initialize();
    
    // Инициализируем логирование  
    Logger::initialize();
    
    // Запускаем сессию
    Session::start();
    
    // Применяем rate limiting для всех запросов
    \App\Middleware\RateLimitMiddleware::handle();

} catch (\Exception $e) {
    // Критическая ошибка инициализации
    error_log("Critical initialization error: " . $e->getMessage());
    http_response_code(500);
    exit('System temporarily unavailable');
}

// Импортируем контроллеры
use App\Controllers\LoginController;
use App\Controllers\AdminController;
use App\Controllers\CartController;
use App\Controllers\SpecificationController;
use App\Controllers\ProductController;

// Создаем роутер
$router = new Router();

// Авторизация
$loginController = new LoginController();
$router->match(['GET', 'POST'], '/login', [$loginController, 'loginAction']);

// Логаут
$router->get('/logout', function() {
    \App\Services\AuthService::destroySession();
    header('Location: /login');
    exit;
});

// Админ-панель (требует аутентификации)
$adminController = new AdminController();
$router->get('/admin', function() use ($adminController) {
    \App\Middleware\AuthMiddleware::requireRole('admin');
    $adminController->indexAction();
});

// Корзина
$cartController = new CartController();
$router->match(['GET', 'POST'], '/cart/add', [$cartController, 'addAction']);
$router->get('/cart', [$cartController, 'viewAction']);
$router->post('/cart/clear', [$cartController, 'clearAction']);
$router->post('/cart/remove', [$cartController, 'removeAction']);
$router->get('/cart/json', [$cartController, 'getJsonAction']);
$router->post('/cart/update', [$cartController, 'updateAction']);

// Спецификации
$specController = new SpecificationController();
$router->match(['GET', 'POST'], '/specification/create', [$specController, 'createAction']);
$router->get('/specification/{id}', [$specController, 'viewAction']);
$router->get('/specifications', [$specController, 'listAction']);
$router->get('/specifications/json', [$specController, 'listJsonAction']);

// Товары
$productController = new ProductController();
$router->get('/shop/product', [$productController, 'viewAction']);

// Статические страницы
$router->get('/', function() {
    \App\Core\Layout::render('home/index', []);
});

$router->get('/shop', function() {
    \App\Core\Layout::render('shop/index', []);
});

// 404
$router->set404(function() {
    http_response_code(404);
    \App\Core\Layout::render('errors/404', []);
});

// Обрабатываем запрос
try {
    $router->dispatch();
} catch (\Exception $e) {
    Logger::error("Router dispatch error", [
        'error' => $e->getMessage(),
        'uri' => $_SERVER['REQUEST_URI'] ?? '',
        'method' => $_SERVER['REQUEST_METHOD'] ?? ''
    ]);
    
    http_response_code(500);
    echo "Internal server error";
}