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
 * @details Call getDetails function to get all data from config handler module
 *          Call create_producer function to create a producer instance with provided kafkaURL
 *          Call create_consumer function to create a consumer instance with provided kafkaURL and config offsets
 *          Start kafka_listener thread to receive the data from communication topic
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
//                QtConcurrent::run(this, &Kafka_Communication_Handler::kafka_consumer_Thread);


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
 * @brief   This function is used to get all required data from config hanlder and store it this moduler global variable
 * @param   None
 * @return  True or false for success or failure state
 * @details Create config object to get Instace of config handler class
 *          Get the KafkaURL and assign into consumer broker and producer broker
 *          Get the communication topic name and assign into COMM_TOPIC
 *          Get the serializer of the communication data and assing into serializer
 *          Get the Deserializer of the communication data and assing into DeSerializer
 *
 */
bool Kafka_Communication_Handler::getDetails(){

    config = &Config_Handler::getInstance();

    //getting the kafka url from config handler and set it to consumer and producer brokers
    consumer_broker = config->getKafkadetail().kafkaURL.toStdString();

    //chcek that char of the string is greater than zero
    if(!consumer_broker.empty() || !std::all_of(consumer_broker.begin(), consumer_broker.end(), ::isspace)){
        producer_broker = consumer_broker;

        //chcek that char of the string is greater than zero
        if(!producer_broker.empty() || !std::all_of(producer_broker.begin(), producer_broker.end(), ::isspace)){

            //getting the communication topic name to used to subscribe.
            COMM_TOPIC = config->getCommunicationdetail().topic_name.toStdString();

            //check that char of the string is greater than zero
            if(!COMM_TOPIC.empty() || !std::all_of(COMM_TOPIC.begin(), COMM_TOPIC.end(), ::isspace)){

                //getting the communication data serializer to used to process the data.
                serializer = config->getCommunicationdetail().serializer.toStdString();

                //check that char of the string is greater than zero
                if(!serializer.empty() || !std::all_of(serializer.begin(), serializer.end(), ::isspace)){

                    //getting the communication data DeSerializer to used to process the data.
                    DeSerializer = config->getCommunicationdetail().deSerializer.toStdString();

                    //check that char of the string is greater than zero
                    if(!DeSerializer.empty() || !std::all_of(DeSerializer.begin(), DeSerializer.end(), ::isspace)){

                        if(config->getInstance().getLogLevel()>LOGLEVELINFO){
                            activityLog<< "Kafka communication handler gets all required data from the config handler ";
                            activityLog<<"producer_broker: "<<QString::fromStdString(producer_broker)
                                      <<"consumer_broker: "<<QString::fromStdString(consumer_broker)
                                     <<"COMM_TOPIC: "<<QString::fromStdString(COMM_TOPIC)
                                    <<"serializer: "<<QString::fromStdString(serializer)
                                   <<"DeSerializer: "<<QString::fromStdString(DeSerializer);
                        }
                        //return true once the all variables are stored.
                        return true;
                    }else {
                        if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                            activityLog<< "DeSerializer is either empty or contains only whitespace: "<<QString::fromStdString(DeSerializer);

                        return false;
                    }


                }else {
                    if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                        activityLog<< "serializer is either empty or contains only whitespace: "<<QString::fromStdString(serializer);

                    return false;
                }

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

//void Kafka_Communication_Handler::kafka_consumer_Thread(){

//    std::string errstr;
//    QString m_brokers = "192.168.2.108:9092";
//    QString m_groupId = "group1";
//    QString m_topic = "TOPIC_A_500_1000";

//     RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
//     conf->set("bootstrap.servers", m_brokers.toStdString(), errstr);
//     conf->set("group.id", m_groupId.toStdString(), errstr);
//     conf->set("auto.offset.reset", "latest", errstr);

//     RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
//     delete conf;

//     if (!consumer) {
//         qCritical() << "Failed to create consumer:" << QString::fromStdString(errstr);
//         return;
//     }

//     std::vector<std::string> topics = { m_topic.toStdString(),"TOPIC_A_1100_1500", "TOPIC_A_1600_2000" };
//     RdKafka::ErrorCode err = consumer->subscribe(topics);
//     if (err != RdKafka::ERR_NO_ERROR) {
//         qCritical() << "Failed to subscribe:" << QString::fromStdString(RdKafka::err2str(err));
//         delete consumer;
//         return;
//     }
//     string topick=m_topic.toStdString();

//RdKafka::Message *msg ;
//     while ((msg = consumer->consume(100)) != nullptr) {
////        RdKafka::Message *msg = consumer->consumer(100);  // timeout 100ms

//        switch (msg->err()) {
//        case RdKafka::ERR_NO_ERROR: {
//            // Key
//            if (msg->key() && msg->key_len() == sizeof(int32_t)) {
//                int32_t keyNetworkOrder;
//                memcpy(&keyNetworkOrder, msg->key_pointer(), sizeof(int32_t));
//                int32_t channelKey = ntohl(keyNetworkOrder);  // convert back from network to host byte order

//                // Value (Protobuf)
//                sensordata::SensorData wrapper;
//                if (wrapper.ParseFromArray(msg->payload(), msg->len())) {
//                    if (wrapper.has_analog()) {
//                        const auto &analog = wrapper.analog();

////                        emit analogDataReceived(analog.channelid(), analog.data());
//                        qDebug() << "Received Analog: CH" << analog.channelid()
//                                 << "Data:" << analog.data() << "counter:" << analog.counter();
//                    }
//                } else {
//                    qWarning() << "Failed to parse protobuf message!";
//                }
//            }
//            break;
//        }
//        case RdKafka::ERR__TIMED_OUT:
//            break;
//        default:
//            qWarning() << "Kafka error:" << QString::fromStdString(msg->errstr());
//            break;
//        }

//        delete msg;
//    }
//}

/**
 * @brief   This function is used to receive the data from communication topic
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

//    QThread::msleep(5000);
//    QJsonObject json;
//    json["key"]=1000;
//    json["acquisitionFlag"]=1;
//    json["message"] = "hi";
//    UpdateSendDataToKafka(json);

    //create nullptr to store the data
    RdKafka::Message *msg = nullptr;

    //Listen the communication topic for 1000ms polling rate
    while ((msg = consumer->consume(Topic, 0, 10)) != nullptr) {
        //check any error is there in the message
        if (msg->err() == RdKafka::ERR_NO_ERROR) {

            //read the data from the message as byteArray
            QByteArray jsonBytes = QByteArray::fromStdString(static_cast<const char*>(msg->payload()));

            //check the data is available in the jsonBytes
            if(jsonBytes.size()>0){

                //check the deserialiser to deserialize the received data
                if(DeSerializer=="JsonObject"){

                    QJsonDocument jsonDoc = QJsonDocument::fromJson(jsonBytes);
                    QJsonObject jsonObject = jsonDoc.object();

                    //emit the signal an pass the jsonobject to the master class slot to further processing
                    emit receivedDataFromKafka(jsonObject);


//                    QThread::msleep(10000);
//                    QJsonObject json;
//                    json["key"]=1000;
//                    json["acquisitionFlag"]=0;
//                    json["message"] = "hi";
//                    UpdateSendDataToKafka(json);

//                    QThread::msleep(5000);
//                    QJsonObject json1;
//                    json1["key"]=1000;
//                    json1["acquisitionFlag"]=1;
//                    json1["message"] = "hi";
//                    UpdateSendDataToKafka(json1);

                }else {
                    if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                        activityLog<<"Cannot process the received data because application expects"
                                     "the deserialier should be JSONOBJECT but actual deserializer is "<<QString::fromStdString(DeSerializer);
}

            }else{
                if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                    activityLog<<"Cannot process the received data from communication topic In kafka_Listener_Thread,"
                                 "because received jsonByte size is zero ";
            }
        }
    }

}

/**
 * @brief   Function to create and configure a Kafka Producer instance
 * @return  success '1' or failure '0'
 * @param   None
 * @details This function is used to Create a producer Instance to publish the data into the topic
 */
int Kafka_Communication_Handler::create_Producer_Instance() {

    // Check if the producer_broker string is not empty and doesn't consist of only whitespaces
    if (!producer_broker.empty() ||
            !std::all_of(producer_broker.begin(), producer_broker.end(), ::isspace)) {

        try {
            // Create a global configuration object for Kafka
            RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

            // Set the broker address in the configuration object
            conf->set("metadata.broker.list", producer_broker, producer_errstr);

            // Create the Kafka Producer using the configuration
            producer = RdKafka::Producer::create(conf, producer_errstr);

            // Check if producer creation failed
            if (!producer) {
                // Log the error message if producer creation failed
                activityLog << "failed to create producer: "
                            << QString::fromStdString(producer_errstr);
                return 0; // Return 0 indicating failure
            } else {
                // If log level is higher than INFO, log the producer creation
                if (config->getInstance().getLogLevel() > LOGLEVELINFO)
                    activityLog << "% Created Producer "
                                << QString::fromStdString(producer->name());
            }

            return 1; // Return 1 indicating successful producer creation

        } catch (const std::bad_alloc &) {
            // Catch memory allocation failure
            activityLog << EXIT_FAILURE; // Log failure
            return 0; // Return 0 indicating failure
        }

    } else {
        // If broker string is empty or only whitespaces, log this information
        if (config->getInstance().getLogLevel() > LOGLEVELINFO)
            activityLog << "cannot create Producer_Instance because producer_broker is either empty or contains only whitespace: "
                        << QString::fromStdString(producer_broker);

        return 0; // Return 0 indicating invalid input for broker address
    }
}


/**
 * @brief   Function to create and configure a Kafka Consumer instance
 * @return  success '1' or failure '0'
 * @param   None
 * @details This function is used to Create consumer Instance to consume the data from the kafka server
 */
int Kafka_Communication_Handler::create_Consumer_Instance() {

    // Check if the consumer_broker string is not empty and not just whitespaces
    if (!consumer_broker.empty() ||
            !std::all_of(consumer_broker.begin(), consumer_broker.end(), ::isspace)) {

        try {
            // Create a global configuration object for Kafka
            RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
            std::string errstr; // Variable to hold error messages

            // Set the broker list in the configuration object
            if (conf->set("metadata.broker.list", consumer_broker, consumer_errstr)
                    != RdKafka::Conf::CONF_OK) {

                // Log the error if broker list setting fails
                activityLog << "Failed to set broker list: "
                            << QString::fromStdString(consumer_errstr);
                // NOTE: 'delete conf;' after 'return' will never execute – dead code
                delete conf;
                return 0; // Return 0 indicating failure


            } else {
                // If log level is above INFO, log the broker list set message
                if (config->getInstance().getLogLevel() > LOGLEVELINFO)
                    activityLog << "set broker list: "
                                << QString::fromStdString(consumer_broker);
            }

            // Try to create the consumer using the configured settings
            consumer = RdKafka::Consumer::create(conf, errstr);

            if (!consumer) {
                // Log error message if consumer creation fails
                activityLog << "Failed to create consumer: "
                            << QString::fromStdString(consumer_errstr);
                delete conf; // Free the config object
                return 0;    // Return 0 indicating failure
            } else {
                // Log consumer creation if log level allows
                if (config->getInstance().getLogLevel() > LOGLEVELINFO)
                    activityLog << "consumer created: " << consumer;
            }

            return 1; // Return 1 indicating success

        } catch (const std::bad_alloc&) {
            // Handle memory allocation failures
            activityLog << EXIT_FAILURE;
            return 0; // Return 0 indicating failure
        }

    } else {
        // If broker string is empty or all whitespaces, log and return failure
        if (config->getInstance().getLogLevel() > LOGLEVELINFO)
            activityLog << "cannot create Consumer_Instance because consumer_broker is either empty or contains only whitespace: "
                        << QString::fromStdString(consumer_broker);

        return 0; // Return 0 indicating invalid broker string
    }
}



/**
 * @brief   Funtion is used to send data to communication topic
 * @param   Json object to send to the communication topic
 * @return  None
 * @details This slot is used to send data to the communication topic, when signal gets emitted from master class
 */
void Kafka_Communication_Handler::UpdateSendDataToKafka(QJsonObject data){

    //Initialize local varibles for produce method
    const void *key=0;
    size_t key_len=0;
    int msgflag = 0;
    int32_t partition  = 0;
    int64_t timestamp =0;

    //check the jsonobject is empty
    if (!data.isEmpty()) {
        //check the serializer
        if(serializer=="JsonObject"){
            //conver the jsonObject to char buffer
            QJsonDocument jsonDoc(data);
            QByteArray byteArray = jsonDoc.toJson();
            char* buffer = byteArray.data();

            // Send Buffer to commuication wopic
            RdKafka::ErrorCode deliveryreport2 = producer->produce(COMM_TOPIC, partition,
                                                                   msgflag, buffer, byteArray.size(),
                                                                   key, key_len, timestamp, NULL);

            //check the delivery report ay error is there
            if(deliveryreport2 !=RdKafka::ERR_NO_ERROR){
                cerr << "failed to produce message : " << RdKafka::err2str(deliveryreport2) << endl;
                activityLog<<"warning : failed to produce message : " << deliveryreport2;
                return;
            }else {
                if(config->getInstance().getLogLevel()>LOGLEVELINFO)
                    activityLog<<"successfully sent data to the communication topic "<<data["key"].toInt()<<"="<<data["message"].toString();
            }
        }else{
            activityLog<<"Cannot send data to communication topic because application expects 'JsonObject' but actual Serializer is "+QString::fromStdString(serializer);
        }
    }

}


