<?php
namespace App\Core;

class Layout
{
    /**
     * Рендерит вьюху внутри общего шаблона с шапкой и подвалом
     * @param string $viewPath Относительный путь от src/views без расширения, например 'cart/view'
     * @param array  $params   Данные для вьюхи
     */
    public static function render(string $viewPath, array $params = []): void
    {
        extract($params, EXTR_SKIP);
        ob_start();
        require __DIR__ . '/../../public/header.php';
        require __DIR__ . '/../../src/views/' . $viewPath . '.php';
        require __DIR__ . '/../../public/footer.php';
        echo ob_get_clean();
    }
}
