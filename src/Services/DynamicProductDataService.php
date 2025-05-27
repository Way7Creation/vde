<?php
namespace App\Services;

use App\Core\Database;
use App\Core\Cache;

/**
 * Сервис для работы с динамическими данными товаров
 * Цены, остатки, доступность - все что часто меняется
 */
class DynamicProductDataService
{
    private const CACHE_TTL = 300; // 5 минут кеш
    private const MAX_BATCH_SIZE = 1000;
    
    /**
     * Получить динамические данные для списка товаров
     * @param array $productIds - массив ID товаров
     * @param int $cityId - ID города
     * @param int|null $userId - ID пользователя (для индивидуальных цен)
     * @return array
     */
    public function getProductsDynamicData(array $productIds, int $cityId, ?int $userId = null): array
    {
        // Валидация входных данных
        $productIds = array_filter($productIds, 'is_numeric');
        $productIds = array_unique($productIds);
        
        if (empty($productIds)) {
            return [];
        }
        
        if (count($productIds) > self::MAX_BATCH_SIZE) {
            throw new \InvalidArgumentException('Слишком много товаров в запросе');
        }
        
        // Проверяем кеш
        $cacheKey = $this->getCacheKey($productIds, $cityId, $userId);
        $cached = Cache::get($cacheKey);
        if ($cached !== null) {
            return $cached;
        }
        
        $pdo = Database::getConnection();
        $result = [];
        
        // 1. Получаем цены (базовые или клиентские)
        $prices = $this->getPrices($pdo, $productIds, $userId);
        
        // 2. Получаем остатки для города
        $stocks = $this->getStocksForCity($pdo, $productIds, $cityId);
        
        // 3. Получаем даты доставки
        $deliveryDates = $this->getDeliveryDates($pdo, $productIds, $cityId, $stocks);
        
        // 4. Собираем результат
        foreach ($productIds as $productId) {
            $result[$productId] = [
                'price' => $prices[$productId] ?? null,
                'stock' => $stocks[$productId] ?? ['quantity' => 0, 'warehouses' => []],
                'delivery' => $deliveryDates[$productId] ?? null,
                'available' => ($stocks[$productId]['quantity'] ?? 0) > 0
            ];
        }
        
        // Сохраняем в кеш
        Cache::set($cacheKey, $result, self::CACHE_TTL);
        
        return $result;
    }
    
    /**
     * Получить цены для товаров
     */
    private function getPrices(PDO $pdo, array $productIds, ?int $userId): array
    {
        $prices = [];
        $placeholders = implode(',', array_fill(0, count($productIds), '?'));
        
        // Если есть пользователь, проверяем его организации и спец цены
        if ($userId) {
            $sql = "
                SELECT 
                    cp.product_id,
                    cp.price as client_price,
                    p.price as base_price
                FROM products prod
                LEFT JOIN prices p ON p.product_id = prod.product_id 
                    AND p.is_base = 1 
                    AND (p.valid_to IS NULL OR p.valid_to >= CURDATE())
                LEFT JOIN clients_organizations co ON co.user_id = ?
                LEFT JOIN client_prices cp ON cp.org_id = co.org_id 
                    AND cp.product_id = prod.product_id
                    AND (cp.valid_to IS NULL OR cp.valid_to >= CURDATE())
                WHERE prod.product_id IN ($placeholders)
                ORDER BY cp.valid_from DESC, p.valid_from DESC
            ";
            
            $stmt = $pdo->prepare($sql);
            $stmt->execute(array_merge([$userId], $productIds));
            
            while ($row = $stmt->fetch()) {
                $productId = $row['product_id'];
                // Используем клиентскую цену если есть, иначе базовую
                $prices[$productId] = [
                    'base' => (float)$row['base_price'],
                    'final' => (float)($row['client_price'] ?? $row['base_price']),
                    'has_special' => !is_null($row['client_price'])
                ];
            }
        } else {
            // Для гостей только базовые цены
            $sql = "
                SELECT product_id, price
                FROM prices
                WHERE product_id IN ($placeholders)
                    AND is_base = 1
                    AND (valid_to IS NULL OR valid_to >= CURDATE())
                ORDER BY valid_from DESC
            ";
            
            $stmt = $pdo->prepare($sql);
            $stmt->execute($productIds);
            
            while ($row = $stmt->fetch()) {
                $prices[$row['product_id']] = [
                    'base' => (float)$row['price'],
                    'final' => (float)$row['price'],
                    'has_special' => false
                ];
            }
        }
        
        return $prices;
    }
    
    /**
     * Получить остатки для города
     */
    private function getStocksForCity(PDO $pdo, array $productIds, int $cityId): array
    {
        $stocks = [];
        $placeholders = implode(',', array_fill(0, count($productIds), '?'));
        
        // Получаем склады города
        $warehouseSql = "
            SELECT DISTINCT w.warehouse_id, w.name
            FROM city_warehouse_mapping cwm
            JOIN warehouses w ON w.warehouse_id = cwm.warehouse_id
            WHERE cwm.city_id = ? AND w.is_active = 1
        ";
        
        $stmt = $pdo->prepare($warehouseSql);
        $stmt->execute([$cityId]);
        $warehouses = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);
        
        if (empty($warehouses)) {
            return $stocks;
        }
        
        $warehouseIds = array_keys($warehouses);
        $whPlaceholders = implode(',', array_fill(0, count($warehouseIds), '?'));
        
        // Получаем остатки
        $stockSql = "
            SELECT 
                sb.product_id,
                sb.warehouse_id,
                w.name as warehouse_name,
                sb.quantity,
                sb.reserved,
                (sb.quantity - sb.reserved) as available
            FROM stock_balances sb
            JOIN warehouses w ON w.warehouse_id = sb.warehouse_id
            WHERE sb.product_id IN ($placeholders)
                AND sb.warehouse_id IN ($whPlaceholders)
                AND sb.quantity > sb.reserved
        ";
        
        $stmt = $pdo->prepare($stockSql);
        $stmt->execute(array_merge($productIds, $warehouseIds));
        
        while ($row = $stmt->fetch()) {
            $productId = $row['product_id'];
            
            if (!isset($stocks[$productId])) {
                $stocks[$productId] = [
                    'quantity' => 0,
                    'warehouses' => []
                ];
            }
            
            $stocks[$productId]['quantity'] += $row['available'];
            $stocks[$productId]['warehouses'][] = [
                'id' => $row['warehouse_id'],
                'name' => $row['warehouse_name'],
                'quantity' => $row['available']
            ];
        }
        
        return $stocks;
    }
    
    /**
     * Получить даты доставки
     */
    private function getDeliveryDates(PDO $pdo, array $productIds, int $cityId, array $stocks): array
    {
        $deliveryDates = [];
        
        // Получаем данные города
        $citySql = "SELECT cutoff_time, delivery_base_days, timezone FROM cities WHERE city_id = ?";
        $stmt = $pdo->prepare($citySql);
        $stmt->execute([$cityId]);
        $cityData = $stmt->fetch();
        
        if (!$cityData) {
            return $deliveryDates;
        }
        
        // Устанавливаем временную зону
        $timezone = new \DateTimeZone($cityData['timezone'] ?? 'Europe/Moscow');
        $now = new \DateTime('now', $timezone);
        $cutoffTime = $cityData['cutoff_time'] ?? '16:00:00';
        
        // Получаем расписания доставки
        $scheduleSql = "
            SELECT 
                ds.warehouse_id,
                ds.delivery_days,
                ds.cutoff_time,
                ds.delivery_mode,
                ds.specific_dates
            FROM delivery_schedules ds
            WHERE ds.city_id = ? 
                AND ds.delivery_type = 1
            ORDER BY ds.is_express DESC
        ";
        
        $stmt = $pdo->prepare($scheduleSql);
        $stmt->execute([$cityId]);
        $schedules = $stmt->fetchAll();
        
        // Рассчитываем даты для каждого товара
        foreach ($productIds as $productId) {
            $hasStock = isset($stocks[$productId]) && $stocks[$productId]['quantity'] > 0;
            $warehouseIds = $hasStock ? array_column($stocks[$productId]['warehouses'], 'id') : [];
            
            $deliveryDate = $this->calculateDeliveryDate(
                $schedules,
                $warehouseIds,
                $now,
                $cutoffTime,
                $cityData['delivery_base_days'] ?? 3,
                !$hasStock
            );
            
            $deliveryDates[$productId] = $deliveryDate;
        }
        
        return $deliveryDates;
    }
    
    /**
     * Рассчитать дату доставки
     */
    private function calculateDeliveryDate(
        array $schedules,
        array $warehouseIds,
        \DateTime $now,
        string $cutoffTime,
        int $baseDays,
        bool $isOnOrder
    ): array {
        $result = [
            'date' => null,
            'text' => 'Уточняйте'
        ];
        
        $currentTime = $now->format('H:i:s');
        $workDate = clone $now;
        
        // Если прошло время отсечки, начинаем с завтра
        if ($currentTime > $cutoffTime) {
            $workDate->modify('+1 day');
        }
        
        // Если товар под заказ, добавляем базовые дни
        if ($isOnOrder) {
            $workDate->modify("+{$baseDays} days");
            $result['text'] = 'Под заказ';
        }
        
        // Ищем ближайшую дату доставки
        $minDate = null;
        
        foreach ($schedules as $schedule) {
            // Если есть склады, проверяем только их расписания
            if (!empty($warehouseIds) && !in_array($schedule['warehouse_id'], $warehouseIds)) {
                continue;
            }
            
            $scheduleDate = $this->getNextDeliveryDate($schedule, $workDate);
            
            if ($scheduleDate && (!$minDate || $scheduleDate < $minDate)) {
                $minDate = $scheduleDate;
            }
        }
        
        if ($minDate) {
            $result['date'] = $minDate->format('d.m.Y');
            $result['text'] = $this->formatDeliveryDate($minDate, $now);
        }
        
        return $result;
    }
    
    /**
     * Получить следующую дату доставки по расписанию
     */
    private function getNextDeliveryDate(array $schedule, \DateTime $fromDate): ?\DateTime
    {
        if ($schedule['delivery_mode'] === 'specific_dates') {
            // Конкретные даты
            $dates = json_decode($schedule['specific_dates'], true);
            if (!is_array($dates)) return null;
            
            foreach ($dates as $dateStr) {
                $date = \DateTime::createFromFormat('Y-m-d', $dateStr);
                if ($date && $date >= $fromDate) {
                    return $date;
                }
            }
        } else {
            // Еженедельное расписание
            $deliveryDays = json_decode($schedule['delivery_days'], true);
            if (!is_array($deliveryDays)) return null;
            
            // Проверяем следующие 14 дней
            $checkDate = clone $fromDate;
            for ($i = 0; $i < 14; $i++) {
                $dayOfWeek = (int)$checkDate->format('N');
                if (in_array($dayOfWeek, $deliveryDays)) {
                    return $checkDate;
                }
                $checkDate->modify('+1 day');
            }
        }
        
        return null;
    }
    
    /**
     * Форматировать дату доставки для отображения
     */
    private function formatDeliveryDate(\DateTime $deliveryDate, \DateTime $now): string
    {
        $diff = $now->diff($deliveryDate)->days;
        
        if ($diff == 0) {
            return 'Сегодня';
        } elseif ($diff == 1) {
            return 'Завтра';
        } elseif ($diff <= 6) {
            $days = ['', 'понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье'];
            return 'В ' . $days[$deliveryDate->format('N')];
        } else {
            return $deliveryDate->format('d.m');
        }
    }
    
    /**
     * Сформировать ключ кеша
     */
    private function getCacheKey(array $productIds, int $cityId, ?int $userId): string
    {
        sort($productIds);
        return 'dynamic_data:' . md5(implode(',', $productIds) . ':' . $cityId . ':' . ($userId ?? 0));
    }
}