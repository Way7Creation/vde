<?php
namespace App\Controllers;

use App\Core\Layout;
use App\Core\CSRF;
use App\Core\Cart;
use App\Core\Database;
use App\Services\AuthService;

class SpecificationController
{
    /**
     * POST /specification/create — создание спецификации из корзины
     */
    public function createAction(): void
    {
        header('Content-Type: application/json; charset=utf-8');
        try {
            // 1. Проверка метода и CSRF
            if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
                http_response_code(405);
                echo json_encode(['success' => false, 'message' => 'Метод не разрешён']);
                return;
            }
            if (!CSRF::validate($_POST['csrf_token'] ?? null)) {
                http_response_code(403);
                echo json_encode(['success' => false, 'message' => 'Неверный CSRF токен']);
                return;
            }

            // 2. Получаем корзину
            $cart = Cart::get();
            if (!$cart || !is_array($cart) || count($cart) === 0) {
                echo json_encode(['success' => false, 'message' => 'Корзина пуста']);
                return;
            }

            // 3. Формируем массив товаров для спецификации
            $items = [];
            foreach ($cart as $item) {
                $productId = (int)($item['product_id'] ?? 0);
                $qty = (int)($item['quantity'] ?? 0);
                if (!$productId || $qty <= 0) continue;
            
                // Ищем цену в таблице prices
                $price = 0;
                $pdo = Database::getConnection();
                $stmt = $pdo->prepare("SELECT price FROM prices WHERE product_id = ? AND is_base = 1 ORDER BY valid_from DESC LIMIT 1");
                $stmt->execute([$productId]);
                $dbPrice = $stmt->fetchColumn();
                if (is_numeric($dbPrice)) $price = round($dbPrice, 2);
            
                $items[] = [
                    'product_id' => $productId,
                    'quantity' => $qty,
                    'price' => $price,
                ];
            }
            if (empty($items)) {
                echo json_encode(['success' => false, 'message' => 'Нет валидных товаров в корзине']);
                return;
            }

            // 4. Сохраняем спецификацию
            if (AuthService::check()) {
                $pdo = Database::getConnection();
                $userId = AuthService::check() ? (int)AuthService::user()['id'] : 0;
                $cart = \App\Services\CartService::load($userId);
                \App\Services\CartService::clear($userId);

                // Создаём заголовок спецификации
                $stmt = $pdo->prepare("INSERT INTO specifications (user_id, created_at) VALUES (?, NOW())");
                $stmt->execute([$userId]);
                $specId = $pdo->lastInsertId();

                // Вставляем товары одним запросом
                $values = [];
                $placeholders = [];
                foreach ($items as $item) {
                    $placeholders[] = "(?, ?, ?, ?)";
                    $values[] = $specId;
                    $values[] = $item['product_id'];
                    $values[] = $item['quantity'];
                    $values[] = $item['price'];
                }
                $sql = "INSERT INTO specification_items (specification_id, product_id, quantity, price) VALUES " . implode(',', $placeholders);
                $stmt = $pdo->prepare($sql);
                $stmt->execute($values);

                Cart::save([]); // очищаем корзину
                echo json_encode(['success' => true, 'specification_id' => $specId]);
                return;
            } else {
                // Гость — сохраняем в сессию
                if (session_status() !== PHP_SESSION_ACTIVE) session_start();
                $specId = uniqid('guest_', true);
                $_SESSION['guest_specifications'][$specId] = [
                    'created_at' => date('Y-m-d H:i:s'),
                    'items' => $items
                ];
                Cart::save([]); // очищаем корзину
                echo json_encode(['success' => true, 'specification_id' => $specId, 'guest' => true]);
                return;
            }
        } catch (\Throwable $e) {
            http_response_code(500);
            echo json_encode(['success' => false, 'message' => 'Ошибка сервера', 'error' => $e->getMessage()]);
        }
    }

    /**
     * GET /specification/{id} — просмотр одной спецификации
     */
    public function viewAction(string $id): void
    {
        if (strpos($id, 'guest_') === 0) {
            // Гостевая спецификация из сессии
            if (session_status() !== PHP_SESSION_ACTIVE) session_start();
            $spec = $_SESSION['guest_specifications'][$id] ?? null;
            if (!$spec) {
                Layout::render('specification/view', ['specification' => null, 'items' => [], 'guest' => true]);
                return;
            }
            Layout::render('specification/view', [
                'specification' => ['specification_id' => $id, 'created_at' => $spec['created_at']],
                'items' => $spec['items'],
                'guest' => true
            ]);
        } else {
            // Обычная спецификация из БД
            $pdo = Database::getConnection();
            $stmt = $pdo->prepare("SELECT * FROM specifications WHERE specification_id = ?");
            $stmt->execute([$id]);
            $spec = $stmt->fetch();
            if (!$spec) {
                Layout::render('specification/view', ['specification' => null, 'items' => [], 'guest' => false]);
                return;
            }
            $stmt = $pdo->prepare("SELECT si.*, p.name FROM specification_items si LEFT JOIN products p ON si.product_id = p.product_id WHERE si.specification_id = ?");
            $stmt->execute([$id]);
            $items = $stmt->fetchAll();
            Layout::render('specification/view', [
                'specification' => $spec,
                'items' => $items,
                'guest' => false
            ]);
        }
    }

    /**
     * GET /specifications — список спецификаций пользователя
     */
    public function listAction(): void
    {
        if (!AuthService::check()) {
            header('Location: /login');
            exit;
        }
        $user = AuthService::user();
        $pdo = Database::getConnection();
        $stmt = $pdo->prepare("SELECT * FROM specifications WHERE user_id = ? ORDER BY created_at DESC");
        $stmt->execute([$user['id']]);
        $specs = $stmt->fetchAll();

        $summaries = [];
        $itemsCount = [];
        foreach ($specs as $spec) {
            $sid = $spec['specification_id'];
            $stmt2 = $pdo->prepare("SELECT SUM(quantity * price) AS total FROM specification_items WHERE specification_id = ?");
            $stmt2->execute([$sid]);
            $summaries[$sid] = $stmt2->fetchColumn() ?: 0;

            $stmt3 = $pdo->prepare("SELECT COUNT(*) FROM specification_items WHERE specification_id = ?");
            $stmt3->execute([$sid]);
            $itemsCount[$sid] = (int)$stmt3->fetchColumn();
        }

        Layout::render('specification/index', [
            'specs' => $specs,
            'summaries' => $summaries,
            'itemsCount' => $itemsCount,
        ]);
    }
}