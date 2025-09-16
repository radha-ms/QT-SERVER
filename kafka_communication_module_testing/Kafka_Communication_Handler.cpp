/**
  ***************************************************************************************************************************************
  * @file    Kafka_Communication_Handler.cpp
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all information about the communication process between DAQ server and kafka server
  *
  * @details 1.Create producer and consumer instance with provided URL.
  *          2.Spawns listening thread to consume data.
  *          3.If any data comes emit the signal to the Master_controller.
  *          4.Upon receiving a signal from master_controller, sends to the clients.
  ******************************************************************************************************************************************/



/*
 *Header file which is included and used
 */
#include "Kafka_Communication_Handler.h"


/**
 * @brief   Kafka_Communication_Handler::Kafka_Communication_Handler constructor
 * @param   None
 * @return  None
 * @details Get the required information form the config handler module
 *          call create_producer method
 *          call create_consumer method
 *          start kafka_listener thread
 */
Kafka_Communication_Handler::Kafka_Communication_Handler(QObject *parent) : QObject(parent)
{

    //Get the required data for this module
    if(getDetails()){
        //call create_Producer_Instance method to create producer with provided kafka URL
        if(create_Producer_Instance()){

            //call create_Consumer_Instance method to create consumer with provided kafka URL and communication topic
            if(create_Consumer_Instance()){

                // Start the kafka_Listener_Thread to receive the data
                QtConcurrent::run(this, &Kafka_Communication_Handler::kafka_Listener_Thread);

            }else {
                activityLog<<"warning : cannot create producer with provided URL";
            }
        }else {
            activityLog<<"warning : cannot create consumer with provided URL";
        }
    }else {
        activityLog<<"warning : failed to assign communication topic details in kafka communication handler";
        exit(0);
    }

}

/**
 * @brief   Kafka_Communication_Handler::getDetails
 * @param   None
 * @return  True or false
 * @details This function is used to get kafka & communication topic related variables from config handler.
 *          If successfully stored it returns true, or else it returns false.
 */
bool Kafka_Communication_Handler::getDetails(){



    //getting the kafka url from config handler and set it to consumer and producer brokers
    consumer_broker = config->getInstance().getKafkadetail().kafkaURL.toStdString();

    //chcek that char of the string is greater than zero
    if(!consumer_broker.empty() || !std::all_of(consumer_broker.begin(), consumer_broker.end(), ::isspace)){
        producer_broker = consumer_broker;

        //chcek that char of the string is greater than zero
        if(!producer_broker.empty() || !std::all_of(producer_broker.begin(), producer_broker.end(), ::isspace)){

            //getting the communication topic name to used to subscribe.
            COMM_TOPIC = config->getInstance().getCommunicationdetail().topic_name.toStdString();

            //chcek that char of the string is greater than zero
            if(!COMM_TOPIC.empty() || !std::all_of(COMM_TOPIC.begin(), COMM_TOPIC.end(), ::isspace)){

                if(config->getInstance().getLogLevel()>LOGLEVELINFO){
                    activityLog<< "Kafka communication handler gets all required data from the config handler ";
                    activityLog<<"producer_broker: "<<QString::fromStdString(producer_broker)
                              <<"consumer_broker: "<<QString::fromStdString(consumer_broker)
                             <<"COMM_TOPIC: "<<QString::fromStdString(COMM_TOPIC);
                }
                //return true once the all variables are stored.
                return true;
            }else {
                if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                    activityLog<< "COMM_TOPIC is either empty or contains only whitespace: "<<QString::fromStdString(COMM_TOPIC);

                return false;
            }
        }else {
            if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                activityLog<< "producer_broker is either empty or contains only whitespace: "<<QString::fromStdString(producer_broker);

            return false;
        }
    }else {
        if(config->getInstance().getLogLevel()>LOGLEVELINFO)
            activityLog<<"consumer_broker is either empty or contains only whitespace: " << QString::fromStdString(consumer_broker);

        return false;
    }
}

/**
 * @brief   Kafka_Communication_Handler::kafka_Listener_Thread
 * @param   None
 * @return  None
 * @details Create a topic with provided value
 *          Setting the consumer offset value for the consumer
 *          Subscribe communication topic and start listening in the communication topic
 *          If data comes emit the signal for processing and pass the json object to the slot
 */
void Kafka_Communication_Handler::kafka_Listener_Thread(){


    // Create a topic with the provided communication topic
    Topic = RdKafka::Topic::create(consumer, COMM_TOPIC, nullptr, consumer_errstr);

    //Chech the topic is successfully created or not
    if (!Topic) {
        activityLog<<"failed to create topic: "<<QString::fromStdString(COMM_TOPIC)<<"error: "<<QString::fromStdString(consumer_errstr);
        delete consumer;
        return;

    }else{
        if(config->getInstance().getLogLevel()>LOGLEVELINFO)
            activityLog<<"topic is created: " << QString::fromStdString(COMM_TOPIC);
    }


    //setting the offset configuration to consume the data form the topic
    RdKafka::ErrorCode err = consumer->start(Topic, 0, RdKafka::Topic::OFFSET_END);

    //check any error
    if (err != RdKafka::ERR_NO_ERROR) {
        activityLog<<"Failed to start consumer: " << err;
        delete Topic;
        delete consumer;
        return;
    }else{
        if(config->getInstance().getLogLevel()>LOGLEVELINFO){
            activityLog<<"kafka Listener started with provided kafka URL ";
        }
    }


    RdKafka::Message *msg = nullptr;
    while ((msg = consumer->consume(Topic, 0, 1000)) != nullptr) {
        if (msg->err() == RdKafka::ERR_NO_ERROR) {
            QByteArray jsonBytes = QByteArray::fromStdString(static_cast<const char*>(msg->payload()));
            if(jsonBytes.size()>0){
                QJsonDocument jsonDoc = QJsonDocument::fromJson(jsonBytes);
                QJsonObject jsonObject = jsonDoc.object();
                emit receivedDataFromKafka(jsonObject);

            }else{
                if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                    activityLog<<"Cannot process the received data from communication topic In kafka_Listener_Thread,"
                                 "because received jsonByte size is zero ";

                QJsonObject json;
                json["key"] = 100;
                json["resend"] = 1;
                json["message"]="cannot process received data, please send again";
                emit UpdateSendDataToKafka(json);
                qDebug()<<"json byte size "<<jsonBytes.size();
            }
        }
    }

}

/**
 * @brief   Kafka_Communication_Handler::create_Producer_Instance
 * @return  success '1' or failure '0'
 * @param   None
 * @details This function is used to Create a producer Instance to publish the data into the topic
 */
int Kafka_Communication_Handler::create_Producer_Instance(){

    if(!producer_broker.empty() || !std::all_of(producer_broker.begin(), producer_broker.end(), ::isspace)){
        try{
            RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
            conf->set("metadata.broker.list", producer_broker, producer_errstr);

            //create the producer instance
            producer = RdKafka::Producer::create(conf, producer_errstr);
            if(!producer){
                activityLog<<"failed to create producer: " << QString::fromStdString(producer_errstr);
                return 0;

            }else{
                if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                    activityLog<<"% Created Producer " << QString::fromStdString(producer->name());
            }


            return 1;

        }catch(const std::bad_alloc &){
            activityLog<<EXIT_FAILURE;
            return  0;
        }
    }else {
        if(config->getInstance().getLogLevel()>LOGLEVELINFO)
            activityLog<< "cannot create Producer_Instance because producer_broker is either empty or contains only whitespace: "<<QString::fromStdString(producer_broker);

        return 0;
    }

}

/**
 * @brief   Kafka_Communication_Handler::create_Consumer_Instance
 * @return  success '1' or failure '0'
 * @param   None
 * @details This function is used to Create consumer Instance to consume the data from the kafka server
 */
int Kafka_Communication_Handler::create_Consumer_Instance(){

    if(!consumer_broker.empty() || !std::all_of(consumer_broker.begin(), consumer_broker.end(), ::isspace)){

    try{
        //create configuration interface for kafka
        RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        std::string errstr;

        // In that configuration Set brokers9IP & Port)
        if (conf->set("metadata.broker.list", consumer_broker, consumer_errstr) != RdKafka::Conf::CONF_OK) {
            activityLog<< "Failed to set broker list: " << QString::fromStdString(consumer_errstr);
            return 0;
            delete conf;

        }else{
            if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                activityLog<<"set broker list: " << QString::fromStdString(consumer_broker);
        }

        // Create Kafka consumer
        consumer = RdKafka::Consumer::create(conf, errstr);
        if (!consumer) {
            activityLog<<"Failed to create consumer: " << QString::fromStdString(consumer_errstr) ;
            delete conf;
            return 0;
        }else{
            if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                activityLog<<"consumer created: " << consumer ;
        }
        return 1;
    }catch(const std::bad_alloc &){
        activityLog<< EXIT_FAILURE;
        return 0;
    }
    }else {
        if(config->getInstance().getLogLevel()>LOGLEVELINFO)
            activityLog<< "cannot create Consumer_Instance because consumer_broker is either empty or contains only whitespace: "<<QString::fromStdString(consumer_broker);

        return 0;
    }
}


/**
 * @brief   Kafka_Communication_Handler::UpdateSendData
 * @param   Json object to send to the server
 * @return  None
 * @details This slot is used to send data to the communication topic, when signal gets emitted from master class
 */
void Kafka_Communication_Handler::UpdateSendDataToKafka(QJsonObject data){

    const void *key=0;
    size_t key_len=0;

    int msgflag = 0;
    int32_t partition  = 0;
    int64_t timestamp =0;

    if (!data.isEmpty()) {
        QJsonDocument jsonDoc(data);
        QByteArray byteArray = jsonDoc.toJson();
        char* buffer = byteArray.data();

        // Send Buffer to Kafka producer
        RdKafka::ErrorCode deliveryreport2 = producer->produce(COMM_TOPIC, partition,
                                                               msgflag, buffer, byteArray.size(),
                                                               key, key_len, timestamp, NULL);

        if(deliveryreport2 !=RdKafka::ERR_NO_ERROR){
            cerr << "failed to produce message : " << RdKafka::err2str(deliveryreport2) << endl;
            activityLog<<"warning : failed to produce message : " << deliveryreport2;
            return;
        }else {
            if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                activityLog<<"successfully sent data to the communication topic ";
        }
    }

}


