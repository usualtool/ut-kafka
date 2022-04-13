<?php
namespace usualtool\Kafka;
use library\UsualToolMysql;
use library\UsualToolMssql;
use library\UsualToolPgsql;
use library\UsualToolSqlite;
class Consumer{
    public function __construct($host='127.0.0.1:9020',$topic='ut-test'){
        $this->host = $host;
        $this->topic = $topic;
    }
    public function Run(){
        $conf = new \RdKafka\Conf();
        $conf->set('group.id','0');
        $server = new \RdKafka\Consumer($conf);
        $server->addBrokers($this->host);
        $topicconf = new \RdKafka\TopicConf();
        $topicconf->set('auto.commit.interval.ms', 100);
        $topicconf->set('offset.store.method', 'broker');
        $topicconf->set('auto.offset.reset', 'earliest');
        $topics = $server->newTopic($this->topic,$topicconf);
        $topics->consumeStart(0,RD_KAFKA_OFFSET_STORED);
        while (true) {
            $message = $topics->consume(0,300*1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    //$message->payload
                    if($this->Isjson($message)):
                        //DB Run
                        $data=json_decode($message,true);
                        if(array_key_exists('data',$data) && array_key_exists('sql',$data)):
                            if($data["data"]=="mysql"):
                                UsualToolMysql\UTMysql::RunSql($data["sql"]);
                            elseif($data["data"]=="mssql"):
                                UsualToolMysql\UTMssql::RunSql($data["sql"]);
                            elseif($data["data"]=="pgsql"):
                                UsualToolMysql\UTPgsql::RunSql($data["sql"]);
                            elseif($data["data"]=="sqlite"):
                                UsualToolMysql\UTSqlite::RunSql($data["sql"]);
                            endif;
                        endif;
                    else:
                        //MQ Run
                        /**
                         * MQ processing
                         */
                    endif;
                    echo $message."\r\n";
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages,will wait for more.\r\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out.\r\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
            }
        }
    }
    public function Isjson($string){
        json_decode($string);
        return (json_last_error() == JSON_ERROR_NONE);
    }
}
