<?php
declare(strict_types=1);

// Включаем подробные сообщения об ошибках
ini_set('display_errors', '1');
ini_set('display_startup_errors', '1');
error_reporting(E_ALL);

require_once __DIR__ . '/../vendor/autoload.php';

use App\Core\Session;
use App\Core\Router;
use App\Controllers\LoginController;
use App\Controllers\AdminController;
use App\Controllers\CartController;
use App\Controllers\SpecificationController;

Session::start();

$router = new Router();

// Авторизация
$loginController = new LoginController();
$router->match(['GET', 'POST'], '/login', [$loginController, 'loginAction']);


// Логаут
$router->get('/logout', function() {
    \App\Core\Session::logout();
    header('Location: /login');
    exit;
});

// Админ-панель
$adminController = new AdminController();
$router->get('/admin', [$adminController, 'indexAction']);

// Корзина
$cartController = new CartController();
$router->match(['GET', 'POST'], '/cart/add', [$cartController, 'addAction']);
$router->get('/cart', [$cartController, 'viewAction']);
$router->post('/cart/clear', [$cartController, 'clearAction']); // Новый маршрут для очистки корзины
$router->post('/cart/remove', [$cartController, 'removeAction']); // Новый маршрут для удаления товара из корзины
$router->get('/cart/json', [$cartController, 'getJsonAction']); // Новый маршрут для получения корзины в JSON формате
$router->post('/cart/update', [$cartController, 'updateAction']);

// Спецификации
$specController = new SpecificationController();
$router->match(['GET', 'POST'], '/specification/create', [$specController, 'createAction']);
$router->get('/specification/{id}', [$specController, 'viewAction']);
$router->get('/specifications', [$specController, 'listAction']);
$router->get('/specifications/json', [$specController, 'listJsonAction']); // Новый маршрут для получения спецификаций в JSON формате


// Статические страницы
$router->get('/', function() {
    \App\Core\Layout::render('home/index', []);
});
$router->get('/shop', function() {
    \App\Core\Layout::render('shop/index', []);
});
use App\Controllers\ProductController;
$productController = new ProductController();
$router->get('/shop/product', [$productController, 'viewAction']);

// 404
$router->set404(function() {
    http_response_code(404);
    \App\Core\Layout::render('errors/404', []);
});

$router->dispatch();