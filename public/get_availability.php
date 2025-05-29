<?php
/**
 * API для получения наличия товаров и дат доставки
 * GET /get_availability.php?city_id=1&product_ids=1,2,3
 */

header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

ini_set('display_errors', 0);
ini_set('log_errors', 1);
error_reporting(E_ALL);

$logFile = '/var/www/www-root/data/logs/get_availability.log';

// Функция для логирования
function logMessage($message, $data = null) {
    global $logFile;
    $timestamp = date('Y-m-d H:i:s');
    $log = "[{$timestamp}] {$message}";
    if ($data !== null) {
        $log .= " | Data: " . json_encode($data, JSON_UNESCAPED_UNICODE);
    }
    file_put_contents($logFile, $log . "\n", FILE_APPEND | LOCK_EX);
}

try {
    // Валидация входных параметров
    $cityIdParam = $_GET['city_id'] ?? '';
    $productIdsParam = $_GET['product_ids'] ?? '';
    
    if (!ctype_digit($cityIdParam) || (int)$cityIdParam <= 0) {
        throw new Exception('Некорректный city_id');
    }
    
    if (empty($productIdsParam)) {
        throw new Exception('Не передан product_ids');
    }
    
    $cityId = (int)$cityIdParam;
    
    // Парсинг и валидация product_ids
    $productIdsRaw = explode(',', $productIdsParam);
    $productIds = [];
    
    foreach ($productIdsRaw as $id) {
        $id = trim($id);
        if (ctype_digit($id) && (int)$id > 0) {
            $productIds[] = (int)$id;
        }
    }
    
    if (empty($productIds)) {
        throw new Exception('Нет валидных product_ids');
    }
    
    // Ограничение количества товаров
    if (count($productIds) > 1000) {
        throw new Exception('Слишком много товаров в запросе (макс. 1000)');
    }
    
    logMessage("Запрос наличия", [
        'city_id' => $cityId,
        'product_ids' => $productIds,
        'count' => count($productIds)
    ]);
    
    // Подключение к БД
    $configFile = '/var/www/www-root/data/config/config_bd.ini';
    if (!file_exists($configFile)) {
        throw new Exception("Конфиг не найден: $configFile");
    }
    
    $cfgAll = parse_ini_file($configFile, true, INI_SCANNER_NORMAL);
    if ($cfgAll === false || !isset($cfgAll['mysql'])) {
        throw new Exception('Ошибка чтения конфигурации БД');
    }
    
    $cfg = array_map(function($v){ return trim($v, "\"'"); }, $cfgAll['mysql']);
    
    $pdo = new PDO(
        "mysql:host={$cfg['host']};dbname={$cfg['database']};charset=utf8mb4",
        $cfg['user'], 
        $cfg['password'],
        [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
        ]
    );
    
    // 1) Получаем склады для города
    $stmt = $pdo->prepare("
        SELECT DISTINCT cwm.warehouse_id, w.name as warehouse_name
        FROM city_warehouse_mapping AS cwm
        JOIN warehouses AS w ON w.warehouse_id = cwm.warehouse_id
        WHERE cwm.city_id = ? AND w.is_active = 1
    ");
    $stmt->execute([$cityId]);
    $warehouses = $stmt->fetchAll();
    
    if (empty($warehouses)) {
        logMessage("Нет складов для города", ['city_id' => $cityId]);
        echo json_encode([]);
        exit;
    }
    
    $warehouseIds = array_column($warehouses, 'warehouse_id');
    logMessage("Найдены склады", ['warehouses' => $warehouses]);
    
    // 2) Получаем остатки товаров на складах
    $whPlaceholders = implode(',', array_fill(0, count($warehouseIds), '?'));
    $prodPlaceholders = implode(',', array_fill(0, count($productIds), '?'));
    
    $sqlStocks = "
        SELECT 
            sb.product_id,
            sb.warehouse_id,
            sb.quantity,
            sb.reserved,
            (sb.quantity - sb.reserved) AS available
        FROM stock_balances AS sb
        WHERE sb.warehouse_id IN ($whPlaceholders)
          AND sb.product_id IN ($prodPlaceholders)
          AND sb.quantity > sb.reserved
        ORDER BY sb.product_id, sb.warehouse_id
    ";
    
    $stmt = $pdo->prepare($sqlStocks);
    $stmt->execute(array_merge($warehouseIds, $productIds));
    $stocksData = $stmt->fetchAll();
    
    // Группируем остатки по товарам
    $stocksByProduct = [];
    foreach ($stocksData as $stock) {
        $pid = $stock['product_id'];
        $wid = $stock['warehouse_id'];
        
        if (!isset($stocksByProduct[$pid])) {
            $stocksByProduct[$pid] = [
                'total' => 0,
                'warehouses' => []
            ];
        }
        
        $stocksByProduct[$pid]['total'] += $stock['available'];
        $stocksByProduct[$pid]['warehouses'][$wid] = $stock['available'];
    }
    
    // 3) Получаем данные города
    $stmt = $pdo->prepare("
        SELECT 
            cutoff_time, 
            working_days, 
            delivery_base_days,
            timezone
        FROM cities
        WHERE city_id = ?
    ");
    $stmt->execute([$cityId]);
    $cityData = $stmt->fetch();
    
    if (!$cityData) {
        throw new Exception("Город с ID={$cityId} не найден");
    }
    
    // Устанавливаем временную зону
    $timezone = $cityData['timezone'] ?? 'Europe/Moscow';
    date_default_timezone_set($timezone);
    
    // 4) Получаем расписания доставки
    $sqlSchedules = "
        SELECT 
            ds.warehouse_id,
            ds.delivery_mode,
            ds.delivery_days,
            ds.specific_dates,
            ds.cutoff_time,
            ds.is_express,
            ds.min_order_amount
        FROM delivery_schedules AS ds
        WHERE ds.warehouse_id IN ($whPlaceholders)
          AND ds.city_id = ?
          AND ds.delivery_type = 1
        ORDER BY ds.is_express DESC, ds.cutoff_time DESC
    ";
    
    $stmt = $pdo->prepare($sqlSchedules);
    $stmt->execute(array_merge($warehouseIds, [$cityId]));
    $schedules = $stmt->fetchAll();
    
    // Группируем расписания по складам
    $schedulesByWarehouse = [];
    foreach ($schedules as $schedule) {
        $wid = $schedule['warehouse_id'];
        if (!isset($schedulesByWarehouse[$wid])) {
            $schedulesByWarehouse[$wid] = [];
        }
        $schedulesByWarehouse[$wid][] = $schedule;
    }
    
    // 5) Функция расчёта ближайшей даты доставки
    function calculateDeliveryDate($schedule, $cityData, $hasStock = true) {
        $now = new DateTime();
        $currentTime = $now->format('H:i:s');
        
        // Определяем время отсечки
        $globalCutoff = $cityData['cutoff_time'] ?? '16:00:00';
        $localCutoff = $schedule['cutoff_time'] ?? $globalCutoff;
        
        // Если текущее время больше времени отсечки, начинаем с завтра
        if ($currentTime > $localCutoff) {
            $now->modify('+1 day');
        }
        
        if ($schedule['delivery_mode'] === 'specific_dates') {
            // Конкретные даты доставки
            $dates = json_decode($schedule['specific_dates'], true);
            if (!is_array($dates)) return null;
            
            sort($dates);
            foreach ($dates as $dateStr) {
                $date = DateTime::createFromFormat('Y-m-d', $dateStr);
                if ($date && $date >= $now) {
                    return $date->format('d.m');
                }
            }
            return null;
        } else {
            // Еженедельное расписание
            $daysOfWeek = json_decode($schedule['delivery_days'], true);
            if (!is_array($daysOfWeek)) return null;
            
            // Добавляем базовые дни доставки, если товара нет в наличии
            $additionalDays = $hasStock ? 0 : ($cityData['delivery_base_days'] ?? 1);
            if ($additionalDays > 0) {
                $now->modify("+{$additionalDays} days");
            }
            
            // Ищем ближайший день доставки (максимум 14 дней вперед)
            for ($i = 0; $i < 14; $i++) {
                $dayOfWeek = (int)$now->format('N'); // 1 = понедельник, 7 = воскресенье
                
                if (in_array($dayOfWeek, $daysOfWeek, true)) {
                    return $now->format('d.m');
                }
                
                $now->modify('+1 day');
            }
            
            return null;
        }
    }
    
    // 6) Формируем результат для каждого товара
    $result = [];
    
    foreach ($productIds as $productId) {
        $hasStock = isset($stocksByProduct[$productId]) && $stocksByProduct[$productId]['total'] > 0;
        $totalStock = $hasStock ? $stocksByProduct[$productId]['total'] : 0;
        
        // Находим ближайшую дату доставки
        $deliveryDate = null;
        $minDate = null;
        
        foreach ($warehouseIds as $warehouseId) {
            // Проверяем есть ли товар на этом складе
            $warehouseHasStock = isset($stocksByProduct[$productId]['warehouses'][$warehouseId]) 
                && $stocksByProduct[$productId]['warehouses'][$warehouseId] > 0;
            
            if (!$warehouseHasStock && $hasStock) {
                // Если товар есть на других складах, пропускаем этот склад
                continue;
            }
            
            // Получаем расписания для склада
            $warehouseSchedules = $schedulesByWarehouse[$warehouseId] ?? [];
            
            foreach ($warehouseSchedules as $schedule) {
                $date = calculateDeliveryDate($schedule, $cityData, $warehouseHasStock);
                
                if ($date !== null) {
                    // Преобразуем в сравнимый формат
                    $dateObj = DateTime::createFromFormat('d.m', $date);
                    if ($dateObj) {
                        $dateObj->setDate((int)date('Y'), $dateObj->format('m'), $dateObj->format('d'));
                        
                        if ($minDate === null || $dateObj < $minDate) {
                            $minDate = $dateObj;
                            $deliveryDate = $date;
                        }
                    }
                }
            }
        }
        
        // Если дата не найдена, ставим дефолтную
        if ($deliveryDate === null) {
            $defaultDays = $cityData['delivery_base_days'] ?? 3;
            $defaultDate = new DateTime();
            $defaultDate->modify("+{$defaultDays} days");
            $deliveryDate = $defaultDate->format('d.m');
        }
        
        $result[$productId] = [
            'quantity' => $totalStock,
            'in_stock' => $hasStock,
            'delivery_date' => $deliveryDate,
            'availability_text' => $hasStock ? 
                ($totalStock > 10 ? 'В наличии' : "Осталось {$totalStock} шт.") : 
                'Под заказ'
        ];
    }
    
    // Добавляем товары, которых нет в результате (нет на складах)
    foreach ($productIds as $productId) {
        if (!isset($result[$productId])) {
            $defaultDays = $cityData['delivery_base_days'] ?? 3;
            $defaultDate = new DateTime();
            $defaultDate->modify("+{$defaultDays} days");
            
            $result[$productId] = [
                'quantity' => 0,
                'in_stock' => false,
                'delivery_date' => $defaultDate->format('d.m'),
                'availability_text' => 'Под заказ'
            ];
        }
    }
    
    logMessage("Результат сформирован", [
        'count' => count($result),
        'sample' => array_slice($result, 0, 3, true)
    ]);
    
    echo json_encode($result, JSON_UNESCAPED_UNICODE | JSON_NUMERIC_CHECK);
    
} catch (Exception $e) {
    $error = $e->getMessage();
    logMessage("ERROR", ['message' => $error, 'trace' => $e->getTraceAsString()]);
    
    http_response_code(500);
    echo json_encode(['error' => $error], JSON_UNESCAPED_UNICODE);
}