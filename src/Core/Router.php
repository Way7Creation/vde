<?php
declare(strict_types=1);
namespace App\Core;

class Router
{
    private array $routes = [];
    private $notFoundHandler = null;

    public function get(string $path, $handler): void { $this->addRoute('GET', $path, $handler); }
    public function post(string $path, $handler): void { $this->addRoute('POST', $path, $handler); } // Добавляем метод post
    public function match(array $methods, string $path, $handler): void { foreach ($methods as $m) $this->addRoute($m, $path, $handler); }
    public function set404(callable $handler): void { $this->notFoundHandler = $handler; }
    private function addRoute(string $method, string $path, $handler): void { $this->routes[$method][$path] = $handler; }

    public function dispatch(): void
    {
        $method = $_SERVER['REQUEST_METHOD'] ?? 'GET';
        $uri    = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH) ?: '/';
        if ($uri !== '/' && str_ends_with($uri, '/')) { $uri = rtrim($uri, '/'); }

        foreach ($this->routes[$method] ?? [] as $routePath => $handler) {
            $pattern = '@^' . preg_replace('/\{(\w+)(:[^}]+)?\}/','(?P<$1>[^/]+)', $routePath) . '$@';
            if (preg_match($pattern, $uri, $matches)) {
                $params = array_filter($matches, 'is_string', ARRAY_FILTER_USE_KEY);
                if (is_array($handler)) {
                    [$class, $action] = $handler;
                    $ctrl = is_object($class) ? $class : new $class();
                    echo $ctrl->{$action}(...array_values($params));
                } else {
                    echo call_user_func($handler, ...array_values($params));
                }
                return;
            }
        }
        http_response_code(404);
        if ($this->notFoundHandler) call_user_func($this->notFoundHandler);
        else echo '404 Not Found';
    }
}