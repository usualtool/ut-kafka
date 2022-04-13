<?php
namespace usualtool\Kafka;
class Producer{
    public function __construct($host='127.0.0.1:9020',$topic='ut-test'){
        $this->host = $host;
        $this->topic = $topic;
    }
    public function Send($message){
        $conf = new \RdKafka\Conf();
        $conf->set("metadata.broker.list",$this->host);
        $producer = new \RdKafka\Producer($conf);
        $topic = $producer->newTopic($this->topic);
        $topic->produce(RD_KAFKA_PARTITION_UA,0,$message);
        $producer->poll(0);
    }
}
