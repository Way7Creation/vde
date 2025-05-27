<?php
namespace App\Controllers;

use App\Core\Cart;
use App\Services\CartService;
use App\Services\AuthService;
use App\Core\CSRF;
use App\Core\Layout;

class CartController
{
    /**
     * POST /cart/add — добавить товар в корзину
     */
    public function addAction(): string
    {
        header('Content-Type: application/json; charset=utf-8');
        $productId = (int)($_POST['productId'] ?? $_POST['product_id'] ?? 0);
        $quantity  = (int)($_POST['quantity']  ?? 1);

        if ($productId <= 0 || $quantity <= 0) {
            http_response_code(400);
            return json_encode(['success' => false, 'message' => 'Некорректные данные']);
        }

        if (AuthService::check()) {
            $userId = AuthService::user()['id'];
            $cart = CartService::load($userId);
            if (!isset($cart[$productId])) {
                $cart[$productId] = ['product_id' => $productId, 'quantity' => 0];
            }
            $cart[$productId]['quantity'] += $quantity;
            CartService::saveToDatabase($userId, $cart);
        } else {
            $cart = Cart::get();
            if (!isset($cart[$productId])) {
                $cart[$productId] = ['product_id' => $productId, 'quantity' => 0];
            }
            $cart[$productId]['quantity'] += $quantity;
            Cart::save($cart);
        }
        return json_encode(['success' => true, 'message' => 'Товар добавлен в корзину']);
    }

    /**
     * GET /cart — страница корзины
     */
    public function viewAction(): void
    {
        if (AuthService::check()) {
            $userId = AuthService::user()['id'];
            $cart = CartService::load($userId);
        } else {
            $cart = Cart::get();
        }
        $productIds = array_keys($cart);
        $products = [];
        if (!empty($productIds)) {
            $client = \OpenSearch\ClientBuilder::create()->build();
            $body = [
                'size' => count($productIds),
                'query' => ['ids' => ['values' => $productIds]],
            ];
            $response = $client->search(['index' => 'products_current', 'body' => $body]);
            foreach ($response['hits']['hits'] as $hit) {
                $products[$hit['_id']] = $hit['_source'];
            }
        }
        $rows = [];
        foreach ($cart as $pid => $item) {
            $rows[] = [
                'product_id' => $pid,
                'name' => $products[$pid]['name'] ?? '',
                'quantity' => $item['quantity'],
                'base_price' => $products[$pid]['base_price'] ?? 0,
            ];
        }
        Layout::render('cart/view', [
            'cartRows' => $rows,
            'cart' => $cart,
            'products' => $products 
        ]);
    }

    /**
     * POST /cart/remove — удалить товар из корзины
     */
    public function removeAction(): string
    {
        header('Content-Type: application/json; charset=utf-8');
        if ($_SERVER['REQUEST_METHOD'] !== 'POST' || !CSRF::validate($_POST['csrf_token'] ?? '')) {
            http_response_code(403);
            return json_encode(['success' => false, 'message' => 'Недоступно']);
        }
        $productId = (int)($_POST['productId'] ?? 0);
        if ($productId <= 0) {
            http_response_code(400);
            return json_encode(['success' => false, 'message' => 'Некорректные данные']);
        }
        if (AuthService::check()) {
            $userId = AuthService::user()['id'];
            $cart = CartService::load($userId);
            unset($cart[$productId]);
            CartService::saveToDatabase($userId, $cart);
        } else {
            $cart = Cart::get();
            unset($cart[$productId]);
            Cart::save($cart);
        }
        return json_encode(['success' => true, 'message' => 'Товар удален из корзины']);
    }

    /**
     * POST /cart/clear — очистить корзину
     */
    public function clearAction(): string
    {
        header('Content-Type: application/json; charset=utf-8');
        if ($_SERVER['REQUEST_METHOD'] !== 'POST' || !CSRF::validate($_POST['csrf_token'] ?? '')) {
            http_response_code(403);
            return json_encode(['success' => false, 'message' => 'Недоступно']);
        }
        if (AuthService::check()) {
            $userId = AuthService::user()['id'];
            CartService::clear($userId);
        } else {
            Cart::clear();
        }
        return json_encode(['success' => true, 'message' => 'Корзина очищена']);
    }

    /**
     * GET /cart/json — получить корзину в JSON формате
     */
    public function getJsonAction(): string
    {
        header('Content-Type: application/json; charset=utf-8');
        if (AuthService::check()) {
            $userId = AuthService::user()['id'];
            $cart = CartService::load($userId);
        } else {
            $cart = Cart::get();
        }
        return json_encode(['cart' => $cart]);
    }
}