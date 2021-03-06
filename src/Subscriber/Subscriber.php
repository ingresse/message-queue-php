<?php

namespace MessageQueuePHP\Subscriber;

use MessageQueuePHP\Subscriber\SubscriberInterface;
use MessageQueuePHP\Subscriber\Consumer\ConsumerInterface;
use MessageQueuePHP\Adapter\AdapterInterface;

class Subscriber implements SubscriberInterface
{
    /**
     * @var AdapterInterface
     */
    private $adapter;

    /**
     * @var ConsumerInterface
     */
    private $consumer;

    /**
     * @var string
     */
    private $queue;

    /**
     * @param AdapterInterface $adapter
     */
    public function __construct(AdapterInterface $adapter)
    {
        $this->adapter = $adapter;
    }

    /**
     * @param ConsumerInterface $consumer
     */
    public function setConsumer(ConsumerInterface $consumer)
    {
        $this->consumer = $consumer;
        return $this;
    }

    /**
     * @param string $queue
     * @return self
     */
    public function subscribe($queue)
    {
        $this->queue = $queue;
        return $this;
    }

    /**
     * @return mixed
     */
    public function consume()
    {
        return $this->adapter->consume(
            $this->queue,
            $this->consumer->getTag(),
            array($this->consumer, 'work')
        );
    }
}
