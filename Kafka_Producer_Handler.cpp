
/**
  ***************************************************************************************************************************************
  * @file    Kafka_Producer_Handler.cpp
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all information about publish the data to the kafka server with respective topics.
  *
  * @details 1.Creates producer instance with provided URL.
  *          2.Spawns producer thread and listening for the data from respective bandwidth queue.
  *          3.If any data comes publish it to the topic with channelId as key at refreshRate frequency.
  ******************************************************************************************************************************************/

#include "Kafka_Producer_Handler.h"

Kafka_Producer_Handler::Kafka_Producer_Handler(QString topicname, int refreshRate, LockFreeQueue<QMap<int, DataPacket>> *bandWidthQueue, QObject *parent)
    : QObject(parent), dataQueue(bandWidthQueue), sendingFrequency(refreshRate), TopicNametoSend(topicname) {}

void Kafka_Producer_Handler::kafka_Producer_Thread() {
    if (TopicNametoSend.isEmpty()) {
        activityLog << "In kafka_Producer_Thread received TopicName is empty, cannot produce real-time data into the kafka server";
        exit(0);
    }

    topicnameREAL = TopicNametoSend.toStdString();
    producer_broker = config->getInstance().getKafkadetail().kafkaURL.toStdString();

    if (!producer_broker.empty() || !std::all_of(producer_broker.begin(), producer_broker.end(), ::isspace)) {
        try {
            int freq = 1000 / sendingFrequency;
            std::string lingerMS = std::to_string(freq);

            RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
            conf->set("metadata.broker.list", producer_broker, producer_errstr);
//            conf->set("queue.buffering.max.ms", "100", producer_errstr);

            producer = RdKafka::Producer::create(conf, producer_errstr);

            if (!producer) {
                activityLog << "failed to create producer: " << QString::fromStdString(producer_errstr);
            } else {
                if (config->getInstance().getLogLevel() > LOGLEVELINFO)
                    activityLog << "% Created Producer " << QString::fromStdString(producer->name());
            }
        } catch (const std::bad_alloc &) {
            activityLog << EXIT_FAILURE;
            activityLog << "failed to create producer instance in kafka producer handler";
        }
    } else {
        if (config->getInstance().getLogLevel() > LOGLEVELINFO)
            activityLog << "cannot create Producer_Instance because producer_broker is either empty or contains only whitespace: "
                        << QString::fromStdString(producer_broker);
    }

    qDebug() << "Kafka producer thread started with: " << TopicNametoSend << dataQueue << sendingFrequency;

    //    QString SubFolderName1 = QDir::currentPath() + "/cvsLog/";
    //    QString filename = "/numerical.csv";
    //    QFile *all_data = new QFile(SubFolderName1 + filename);
    //    all_data->open((QIODevice::WriteOnly));
    //    QTextStream log_data_ss1(all_data);

    while (true) {
        //QMap<int, DataPacket> receiveMap = dataQueue->deQueue();
        /* QMap<int, DataPacket> receiveMap;
        if (dataQueue->deQueue(receiveMap)) {

            for (auto it = receiveMap.constBegin(); it != receiveMap.constEnd(); ++it) {
                int channelID_cardID = it.key();
                const DataPacket &packet = it.value();
                int32_t key = htonl(channelID_cardID);

                // DEBUG: Log before publishing
                //            qDebug() << "[INFO] Preparing to send channelID_cardID:" << channelID_cardID
                //                     << " to topic:" << QString::fromStdString(topicnameREAL);

                if (std::holds_alternative<ANALOG_DATA>(packet)) {
                    const ANALOG_DATA &analogData = std::get<ANALOG_DATA>(packet);
                    sensordata::AnalogData protoAnalog;

                    protoAnalog.set_channeltype(analogData.channelType);
                    protoAnalog.set_channelid(analogData.channelID);
                    protoAnalog.set_timestamp(analogData.timestamp);
                    protoAnalog.set_counter(analogData.counter);
                    protoAnalog.set_data(analogData.data);

                    sensordata::SensorData wrapper;
                    wrapper.mutable_analog()->CopyFrom(protoAnalog);

                    std::string serializedData;
                    wrapper.SerializeToString(&serializedData);

                    RdKafka::ErrorCode deliveryreport2 = producer->produce(
                                topicnameREAL, -1,
                                RdKafka::Producer::RK_MSG_COPY,
                                const_cast<char *>(serializedData.data()), serializedData.size(),
                                &key, sizeof(int32_t),
                                0, nullptr);

                    if (deliveryreport2 != RdKafka::ERR_NO_ERROR) {
                        activityLog << "warning : failed to produce message for analog: " << deliveryreport2;
                        return;
                    } else {
                    //    qint64 currentUtcTime = QDateTime::currentDateTimeUtc().toMSecsSinceEpoch();
//                        log_data_ss1 << QString::number(analogData.data, 'f', 6) << ","
//                                     << QString::number(analogData.counter) << "\n";
//                        log_data_ss1.flush();

//                                            qDebug() << "[PUBLISHED] Analog channelID:" << analogData.channelID
//                                                     << " Data:" << analogData.data
//                                                     << " To Topic:" << QString::fromStdString(topicnameREAL);
                    }

                }*/
        QMap<int, DataPacket> receiveMap;
        if (dataQueue->deQueue(receiveMap)) {
            for (auto it = receiveMap.constBegin(); it != receiveMap.constEnd(); ++it) {
                int channelID_cardID = it.key();
                const DataPacket &packet = it.value();
                int32_t key = htonl(channelID_cardID);

                if (std::holds_alternative<ANALOG_DATA>(packet)) {
                    const ANALOG_DATA &analogData = std::get<ANALOG_DATA>(packet);

                    // Build JSON object
                    QJsonObject jsonObj;
                    jsonObj["type"] = "analog";
                    jsonObj["channelType"] = analogData.channelType;
                    jsonObj["channelID"] = analogData.channelID;
                    jsonObj["timestamp"] = QString::number(analogData.timestamp);
                    jsonObj["counter"] = analogData.counter;
                    jsonObj["data"] = analogData.data;

                    QJsonDocument doc(jsonObj);
                    QByteArray jsonBytes = doc.toJson(QJsonDocument::Compact);
                    //    qDebug()<<"SIZE OF BYTE ARRAY BEFORE PUB:"<<jsonBytes.size();

                    RdKafka::ErrorCode deliveryReport = producer->produce(
                                topicnameREAL,
                                -1,
                                RdKafka::Producer::RK_MSG_COPY,
                                jsonBytes.data(),
                                jsonBytes.size(),
                                &key,
                                sizeof(int32_t),
                                analogData.timestamp,
                                nullptr);

                    if (deliveryReport != RdKafka::ERR_NO_ERROR) {
                        activityLog << "JSON publish failed for analog: " << deliveryReport;
                        return;
                    }else {
                        qDebug()<<"data was send to kafka topic "<<analogData.channelID<<" "<<topicnameREAL.c_str();
                    }
                    }
                                  else if (std::holds_alternative<DIGITAL_DATA>(packet)) {
                                  const DIGITAL_DATA &digitalData = std::get<DIGITAL_DATA>(packet);

                                  QJsonObject jsonObj;
                                  jsonObj["type"] = "digital";
                        jsonObj["channelType"] = digitalData.channelType;
                        jsonObj["cardID"] = digitalData.cardID;
                        jsonObj["timestamp"] = QString::number(digitalData.timestamp);
                        jsonObj["counter"] = digitalData.counter;

                        // Add 64 digital bits as an array
                        QJsonArray dataArray;
                        for (int i = 0; i < 64; ++i)
                            dataArray.append(static_cast<int>(digitalData.data[i]));

                        jsonObj["data"] = dataArray;

                        QJsonDocument doc(jsonObj);
                        QByteArray jsonBytes = doc.toJson(QJsonDocument::Compact);
                        RdKafka::ErrorCode deliveryreport2 = producer->produce(
                                    topicnameREAL, -1,
                                    RdKafka::Producer::RK_MSG_COPY,
                                    jsonBytes.data(), jsonBytes.size(),
                                    &key, sizeof(int32_t),
                                    digitalData.timestamp, nullptr);

                        if (deliveryreport2 != RdKafka::ERR_NO_ERROR) {
                            activityLog << "warning : failed to produce message for digital: " << deliveryreport2;
                            return;
                        } else {
                            qDebug() << "[PUBLISHED] Digital cardID:" << digitalData.cardID
                                     << " To Topic:" << QString::fromStdString(topicnameREAL);

                        }
                    }

                    //        else if (std::holds_alternative<DIGITAL_DATA>(packet)) {
                    //                    const DIGITAL_DATA &digitalData = std::get<DIGITAL_DATA>(packet);

                    //                    sensordata::DigitalData protoDigital;
                    //                    protoDigital.set_channeltype(digitalData.channelType);
                    //                    protoDigital.set_cardid(digitalData.cardID);
                    //                    protoDigital.set_timestamp(digitalData.timestamp);
                    //                    protoDigital.set_counter(digitalData.counter);

                    //                    for (int i = 0; i < 64; ++i)
                    //                        protoDigital.add_data(digitalData.data[i]);

                    //                    sensordata::SensorData wrapper;
                    //                    wrapper.mutable_digital()->CopyFrom(protoDigital);

                    //                    std::string serializedData;
                    //                    wrapper.SerializeToString(&serializedData);

                    //                    RdKafka::ErrorCode deliveryreport2 = producer->produce(
                    //                                topicnameREAL, -1,
                    //                                RdKafka::Producer::RK_MSG_COPY,
                    //                                const_cast<char *>(serializedData.data()), serializedData.size(),
                    //                                &key, sizeof(int32_t),
                    //                                0, nullptr);

                    //                    if (deliveryreport2 != RdKafka::ERR_NO_ERROR) {
                    //                        activityLog << "warning : failed to produce message for digital: " << deliveryreport2;
                    //                        return;
                    //                    } else {
                    //                        qDebug() << "[PUBLISHED] Digital cardID:" << digitalData.cardID
                    //                                 << " To Topic:" << QString::fromStdString(topicnameREAL);
                    //                    }
                    //                }
                }
            }
        }
    }



    ///**
    //  ***************************************************************************************************************************************
    //  * @file    Kafka_Producer_Handler.cpp
    //  * @author  karthikeya.sr
    //  * @version V1.0
    //  * @date    02-May-2025
    //  * @brief   This file provides all information about publish the data to the kafka server with respective topics.
    //  *
    //  * @details 1.Creates producer instance with provided URL.
    //  *          2.Spawns producer thread and listening for the data from respective bandwidth queue.
    //  *          3.If any data comes publish it to the topic with channelId as key at refreshRate frequency.
    //  *
    //  ******************************************************************************************************************************************/



    ///*
    // *Header file which is included and used
    // */
    //#include "Kafka_Producer_Handler.h"


    ///**
    // * @brief Kafka_Producer_Handler constructor
    // * @param parent
    // * @return None
    // * @details Take the kafkaURL from config_hander and validate the value,
    // *          If the value is invalid log the error and exit
    // *          Create producer instance for each topic
    // *          Configure the refreshRate as a linger.ms in the producer instance
    // *          Execute the producer function
    // */
    //Kafka_Producer_Handler::Kafka_Producer_Handler(QString topicname,int refreshRate,  Queue_Manager<QMap<int, DataPacket>> *bandWidthQueue,QObject *parent)
    //    : QObject(parent) , dataQueue(bandWidthQueue),sendingFrequency(refreshRate), TopicNametoSend(topicname)
    //{


    //}

    ///**
    // * @brief This function is used to send the real-time data to the kafka topic
    // * @return None
    // * @details Listens in the queue, If any data comes receive as map
    // *          Take the key from the map and assign as a payload key of the procude API
    // *          Take the value from the map check the data is analog or digital
    // *          And assign as a payload of the produce API
    // *          write the data into the topicname
    // */
    //void Kafka_Producer_Handler::kafka_Producer_Thread(){

    //    //check that topic name is valid or not
    //    if(TopicNametoSend.isEmpty()){
    //        activityLog<<"In kafka_Producer_Thread received TopicName is empty, cannot produce real-time data into the kafka server";
    //        exit(0);
    //    }

    //    //convert QString to std string to use in produce API
    //    topicnameREAL = TopicNametoSend.toStdString();
    //    //Get the kafka url from config handler
    //    producer_broker = config->getInstance().getKafkadetail().kafkaURL.toStdString();

    //    // Check if the producer_broker string is not empty and doesn't consist of only whitespaces
    //    if (!producer_broker.empty() ||
    //            !std::all_of(producer_broker.begin(), producer_broker.end(), ::isspace)) {

    //        try {
    //            //process the refreshRate like hz. It should be calculated by ms in 1 sec
    //            int freq = 1000/sendingFrequency;
    //            std::string lingerMS = std::to_string(freq);
    //            // Create a global configuration object for Kafka
    //            RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    //            // Set the broker address in the configuration object
    //            conf->set("metadata.broker.list", producer_broker, producer_errstr);
    //            conf->set("queue.buffering.max.ms", "100" , producer_errstr); // Wait refreshRate time before sending batch

    //            // Create the Kafka Producer using the configuration
    //            producer = RdKafka::Producer::create(conf, producer_errstr);

    //            // Check if producer creation failed
    //            if (!producer) {
    //                // Log the error message if producer creation failed
    //                activityLog << "failed to create producer: "
    //                            << QString::fromStdString(producer_errstr);
    //            } else {
    //                // If log level is higher than INFO, log the producer creation
    //                if (config->getInstance().getLogLevel() > LOGLEVELINFO)
    //                    activityLog << "% Created Producer "
    //                                << QString::fromStdString(producer->name());
    //            }


    //        } catch (const std::bad_alloc &) {
    //            // Catch memory allocation failure
    //            activityLog << EXIT_FAILURE; // Log failure
    //            activityLog<<"failed to create producer instance in kafka producer handler";
    //        }
    //    } else {
    //        // If broker string is empty or only whitespaces, log this information
    //        if (config->getInstance().getLogLevel() > LOGLEVELINFO)
    //            activityLog << "cannot create Producer_Instance because producer_broker is either empty or contains only whitespace: "
    //                        << QString::fromStdString(producer_broker);

    //    }


    //    qDebug()<<"kafka producer thread get started with : "<<TopicNametoSend <<dataQueue << sendingFrequency;


    //    //local variable for each producer instance to produce data

    //    int32_t partition  = -1;
    //    int64_t timestamp =0;

    //    int threadID = reinterpret_cast<quintptr>(QThread::currentThreadId());
    //    qDebug() << "Kafka producer thread waiting for AcqFlag...";

    //    // Wait until AcqFlag is 1
    //    //      {
    //    //          QMutexLocker locker(&config->getInstance().acqMutex);
    //    //          while (config->getInstance().getAcqFlag() == 0) {
    //    //              config->getInstance().acqCondition.wait(&config->getInstance().acqMutex);
    //    //          }
    //    //      }

    //    qDebug() << "Kafka producer thread started with: " << TopicNametoSend << dataQueue << sendingFrequency;

    //    QString SubFolderName1 = QDir::currentPath()+"/cvsLog/";

    //    // qt csv
    //    //    QString filename = "/"+QDateTime::currentDateTime().toString("yyyyMMddhhmmss")+"-QT.csv";
    //    QString filename = "/numerical.csv";

    //    QFile *all_data = new QFile(SubFolderName1+filename);
    //    all_data->open((QIODevice :: WriteOnly));
    //    //    qDebug()<<"File writing started";
    //    QTextStream log_data_ss1(all_data);


    //    while (true) {

    //        // Check periodically if AcqFlag becomes 0 to stop
    //        //               if (config->getInstance().getAcqFlag() == 0) {
    //        //                   qDebug() << "AcqFlag became 0, Kafka Producer waiting again...";
    //        //                   QMutexLocker locker(&config->getInstance().acqMutex);
    //        //                   while (config->getInstance().getAcqFlag() == 0) {
    //        //                       config->getInstance().acqCondition.wait(&config->getInstance().acqMutex);
    //        //                   }
    //        //                   qDebug() << "Kafka Producer thread resumed after AcqFlag set to 1.";
    //        //                   continue;
    //        //               }

    //        //            qDebug() << "Thread ID:" << threadID << " Topic:" << TopicNametoSend << " running...";


    //        QMap<int, DataPacket> receiveMap = dataQueue->dequeue();
    //        //        qDebug()<<"data is there in kafka producer thread";

    //        // Iterate over each channelID_cardID and corresponding DataPacket in the map
    //        for (auto it = receiveMap.constBegin(); it != receiveMap.constEnd(); ++it) {
    //            int channelID_cardID = it.key();
    //            const DataPacket& packet = it.value();

    //            int32_t key = htonl(channelID_cardID);


    //            if (std::holds_alternative<ANALOG_DATA>(packet)) {
    //                const ANALOG_DATA& analogData = std::get<ANALOG_DATA>(packet);
    //                sensordata::AnalogData protoAnalog;

    //                protoAnalog.set_channeltype(analogData.channelType);
    //                protoAnalog.set_channelid(analogData.channelID);
    //                protoAnalog.set_timestamp(analogData.timestamp);
    //                protoAnalog.set_counter(analogData.counter);
    //                protoAnalog.set_data(analogData.data);

    //                sensordata::SensorData wrapper;
    //                wrapper.mutable_analog()->CopyFrom(protoAnalog);

    //                std::string serializedData;
    //                wrapper.SerializeToString(&serializedData);


    //                RdKafka::ErrorCode deliveryreport2 = producer->produce(
    //                            topicnameREAL, partition,
    //                            RdKafka::Producer::RK_MSG_COPY,
    //                            const_cast<char*>(serializedData.data()), serializedData.size(),
    //                            &key, sizeof(int32_t),
    //                            timestamp,
    //                            nullptr
    //                            );


    //                //                cout<<producer->outq_len()<<endl;
    //                //check the delivery report ay error is there
    //                if(deliveryreport2 !=RdKafka::ERR_NO_ERROR){
    //                    activityLog<<"warning : failed to produce message for analog: " << deliveryreport2;
    //                    return;
    //                }else {
    //                  //  qDebug()<<"successfully sent analog data to the kafka topic "<<QString::fromStdString(topicnameREAL)<<" "<<channelID_cardID<<" "<<" "<<analogData.data<<" "<<analogData.timestamp<<" "<<analogData.counter;
    //                    // qDebug()<<analogData.data;
    //                    //                   qint64 currentUtcTime = QDateTime::currentDateTimeUtc().toMSecsSinceEpoch();
    //                    //                    log_data_ss1<<QString::number(analogData.counter)+",";
    //                    //                    log_data_ss1<<QString::number(analogData.timestamp)+",";
    //                    //                   log_data_ss1<<QString::number(currentUtcTime);
    //                    //                    log_data_ss1 <<"\n";
    //                    //                    log_data_ss1.flush();

    //                    qint64 currentUtcTime = QDateTime::currentDateTimeUtc().toMSecsSinceEpoch();
    //                    log_data_ss1
    //                            //                        << QString::number(analogData.channelID) << ","
    //                            //                        << QString::number(analogData.channelType) << ","
    //                            << QString::number(analogData.data, 'f', 6) << ","
    //                               //                        << QString::number(analogData.timestamp) << ","
    //                            << QString::number(analogData.counter) << ","
    //                               //                        << QString::number(currentUtcTime)
    //                            << "\n";
    //                    log_data_ss1.flush();

    //                }

    //            } else if (std::holds_alternative<DIGITAL_DATA>(packet)) {
    //                const DIGITAL_DATA& digitalData = std::get<DIGITAL_DATA>(packet);


    //                sensordata::DigitalData protoDigital;
    //                protoDigital.set_channeltype(digitalData.channelType);
    //                protoDigital.set_cardid(digitalData.cardID);
    //                protoDigital.set_timestamp(digitalData.timestamp);
    //                protoDigital.set_counter(digitalData.counter);

    //                for (int i = 0; i < 64; ++i)
    //                    protoDigital.add_data(digitalData.data[i]);

    //                sensordata::SensorData wrapper;
    //                wrapper.mutable_digital()->CopyFrom(protoDigital);

    //                std::string serializedData;
    //                wrapper.SerializeToString(&serializedData);

    //                RdKafka::ErrorCode deliveryreport2 = producer->produce(
    //                            topicnameREAL, partition,
    //                            RdKafka::Producer::RK_MSG_COPY,
    //                            const_cast<char*>(serializedData.data()), serializedData.size(),
    //                            &key, sizeof(int32_t),
    //                            timestamp,
    //                            nullptr
    //                            );


    //                cout<<producer->outq_len();
    //                //check the delivery report ay error is there
    //                if(deliveryreport2 !=RdKafka::ERR_NO_ERROR){
    //                    activityLog<<"warning : failed to produce message for digital: " << deliveryreport2;
    //                    return;
    //                }else {
    //                    //                    if(config->getInstance().getLogLevel()>LOGLEVELINFO)
    //                    qDebug()<<"successfully sent digitalData data to the kafka topic "<<" "<<QString::fromStdString(topicnameREAL)<<" "<<" "<<channelID_cardID<<" "<<" "<<digitalData.data<<" "<<digitalData.timestamp<<" "<<digitalData.counter;
    //                }
    //            }
    //        }
    //    }


    //}
