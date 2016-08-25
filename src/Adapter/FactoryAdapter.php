<?php

namespace MessageQueuePHP\Adapter;

use MessageQueuePHP\Config\Config;
use MessageQueuePHP\Adapter\AMQPAdapter;

class FactoryAdapter
{
    public function __construct(array $params)
    {
        return new AMQPAdapter(new Config($params));
    }
}