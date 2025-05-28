<?php
namespace App\Core;

use PDO;
use PDOException;

class Database {
    private static ?PDO $pdo = null;

    public static function getConnection(): PDO {
        if (self::$pdo !== null) {
            return self::$pdo;
        }
        $conf = parse_ini_file('/var/www/www-root/data/config/config_bd.ini', true)['mysql'];
        $dsn = "mysql:host={$conf['host']};dbname={$conf['database']};charset=utf8mb4";
        try {
            self::$pdo = new PDO($dsn, $conf['user'], $conf['password'], [
                PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            ]);
        } catch (PDOException $e) {
            exit('Database connection error: '.$e->getMessage());
        }
        return self::$pdo;
    }
}
