<?php
namespace App\Core;

class SearchConfig
{
    // Используем алиас вместо прямого указания версии
    const PRODUCTS_INDEX = 'products_current';
    const PRODUCTS_INDEX_VERSION = 'products_v4';
    
    // Настройки поиска
    const DEFAULT_PAGE_SIZE = 20;
    const MAX_PAGE_SIZE = 100;
    const SEARCH_TIMEOUT = 5000; // мс
    
    /**
     * Получить клиент OpenSearch
     */
    public static function getClient(): \OpenSearch\Client
    {
        static $client = null;
        
        if ($client === null) {
            $client = \OpenSearch\ClientBuilder::create()
                ->setHosts(['localhost:9200'])
                ->setRetries(2)
                ->build();
        }
        
        return $client;
    }
}

// Затем везде в коде замените:
// Было: $client->search(['index' => 'products_v2', ...])
// Стало: $client->search(['index' => SearchConfig::PRODUCTS_INDEX, ...])
