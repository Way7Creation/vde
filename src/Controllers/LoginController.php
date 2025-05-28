<?php
namespace App\Controllers;

use App\Core\Layout;
use App\Services\AuthService;
use App\Core\CSRF;

class LoginController
{
    public function loginAction(): void
    {
        $error = '';

        if ($_SERVER['REQUEST_METHOD'] === 'POST') {
            if (!CSRF::validate($_POST['csrf_token'] ?? null)) {
                $error = 'Неверный CSRF-токен';
            } else {
                $usernameOrEmail = trim($_POST['username'] ?? '');
                $password = $_POST['password'] ?? '';

                [$ok, $msg] = AuthService::login($usernameOrEmail, $password);

                if ($ok) {
                    header('Location: /admin');
                    exit;
                } else {
                    $error = $msg;
                }
            }
        }

        Layout::render('auth/login', ['error' => $error]);
    }
}
