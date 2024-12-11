<?php

namespace MessageQueuePHP\Adapter;

use MessageQueuePHP\Adapter\AdapterInterface;
use MessageQueuePHP\Config\ConfigInterface;
use MessageQueuePHP\Message\Message;
use MessageQueuePHP\Logger\QueueLogger;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Exception;

class AMQPAdapter implements AdapterInterface
{
    /**
     * @var MessageQueuePHP\Config\ConfigInterface
     */
    private $config;
    /**
     * @var PhpAmqpLib\Connection\AMQPStreamConnection
     */
    private $connection;
    /**
     * @var PhpAmqpLib\Channel\AMQPChannel
     */
    private $channel;
    /**
     * @var string
     */
    private $exchange;
    /**
     * @var array
     */
    private $queues = [];
    /**
     * @var Ingresse\Logger\QueueLogger
     */
    public $logger;
    /**
     * @var string
     */
    private $consumerTag;
    /**
     * @var array
     */
    private $messages = [];

    /**
     * @param MessageQueuePHP\Config\ConfigInterface $config
     */
    public function __construct(ConfigInterface $config)
    {
        $this->config     = $config->getConfig();
        $this->logger     = $this->prepareLogger($config);
        $this->connection = $this->prepareConnection();
        $this->channel    = $this->connection->channel();
        $this->setQueues();
        $this->setExchanges();
    }


    /**
     * @return PhpAmqpLib\Connection\AMQPStreamConnection
     */
    private function prepareConnection()
    {
        return new AMQPStreamConnection(
            $this->config['connection']['host'],
            $this->config['connection']['port'],
            $this->config['connection']['user'],
            $this->config['connection']['pass'],
            $this->config['connection']['vhost']
        );
    }

    /**
     * @param  MessageQueuePHP\Config\ConfigInterface $config
     * @return MessageQueuePHP\Logger\QueueLogger
     */
    private function prepareLogger($config)
    {
        if (!isset($this->config['logger'])) {
            return null;
        }
        return new QueueLogger($config);
    }

    /**
     * @param string $exchange
     * @param string $type
     */
    public function setExchange($exchange, $type)
    {
        $this->exchange = $exchange;
        $this->channel->exchange_declare($exchange, $type);
    }

    /**
     * @return string
     */
    public function getExchange()
    {
        return $this->exchange;
    }

    /**
     * @return array
     */
    public function getQueues()
    {
        return $this->queues;
    }

    /**
     * @param  string $queue
     * @param  string $exchange
     * @return self
     */
    public function bind($queue, $exchange)
    {
        $this->channel->queue_bind($queue, $exchange);
        return $this;
    }

    /**
     * @param  MessageQueuePHP\Message\Message $message
     * @param  string                          $queue
     * @param  string                          $exchange
     * @return void
     */
    public function send(Message $message, $queue, $exchange)
    {
        $amqpMessage = new AMQPMessage($message->getPayload(), $message->getProperties());

        $this->defineDeliveryMode($amqpMessage, $queue, $exchange);

        try {
            $this->channel->basic_publish(
                $amqpMessage,
                $exchange,
                $queue
            );
        } catch (Exception $exception){
            $this->logger->setMessage(
                $exception->getMessage(),
                'warning'
            );
            throw $exception;
        }
    }

    /**
     * @param  string        $queue
     * @param  string        $consumeTag
     * @param  callable|null $callBack
     * @return void
     */
    public function consume($queue, $consumeTag, $callBack)
    {
        $params = $this->config['consume'][$consumeTag];
        $this->channel->basic_consume(
            $queue,
            $consumeTag,
            $params['noLocal'],
            $params['noAck'],
            $params['exclusive'],
            $params['noWait'],
            $callBack
        );

        while(count($this->channel->callbacks)) {
            $this->channel->wait();
        }
        $this->close();
    }

    /**
     * @param  string        $queue
     * @param  string        $consumeTag
     * @return void
     */
    public function receive($queue, $consumeTag, $timeout = 0)
    {
        if (empty($this->consumerTag)) {
            $params = $this->config['consume'][$consumeTag];
            $this->channel->basic_consume(
                $queue,
                $consumeTag,
                $params['noLocal'],
                $params['noAck'],
                $params['exclusive'],
                $params['noWait'],
                function (AMQPMessage $message) {
                    $this->messages[] = $message;
                }
            );

            $this->consumerTag = $consumeTag;
        }

        if ($message = $this->getMessage()) {
            return $message;
        }

        while (true) {
            $start = microtime(true);

            if ($message = $this->getMessage()) {
                return $message;
            }

            $this->channel->wait(null, false, $timeout / 1000);

            if ($timeout == 0) {
                continue;
            }

            $stop = microtime(true);
            $timeout -= ($stop - $start) * 1000;

            if ($timeout <= 0) {
                break;
            }
        }
    }

    /**
     * @return void
     */
    public function close()
    {
        $this->channel->close();
        $this->connection->close();
    }

    /**
     * @return void
     */
    private function setQueues()
    {
        foreach ($this->config['queues'] as $queue => $params) {
            $this->queues[] = $this->channel->queue_declare(
                $queue,
                $params['passive'],
                $params['durable'],
                $params['exclusive'],
                $params['autoDelete']
            );
        }
    }

    /**
     * @return void
     */
    private function setExchanges()
    {
        foreach ($this->config['exchanges'] as $exchange => $params) {
            $this->exchanges[] = $this->channel->exchange_declare(
                $exchange,
                $params['type'],
                $params['passive'],
                $params['durable'],
                $params['auto_delete'],
                $params['internal'],
                $params['nowait'],
                $params['arguments'],
                $params['ticket']
            );
        }
    }

    /**
     * @param  PhpAmqpLib\Message\AMQPMessage $message
     * @param  string                         $queue
     * @param  string                         $exchange
     * @return void
     */
    private function defineDeliveryMode(&$message, $queue, $exchange)
    {
        if (!empty($exchange)) {
            $params = $this->config['exchanges'][$exchange];
            $message->set('delivery_mode', $params['delivery_mode']);
            return;
        }

        $params = $this->config['queues'][$queue];
        $message->set('delivery_mode', $params['delivery_mode']);
    }

    /**
     * @param  string $consumerTag
     * @return Message|null
     */
    private function getMessage()
    {
        if (!empty($this->messages)) {
            return array_shift($this->messages);
        }
    }
}
