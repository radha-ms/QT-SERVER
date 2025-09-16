/**
  ***********************************************************************************************************************************
  * @file   Server_Master_Controller.h
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provide all header to Master.cpp
  ***************************************************************************************************************************************/


#ifndef MASTER_CONTROLLER_H
#define MASTER_CONTROLLER_H

/*
 *Header file which is included and used
 */
#include <QObject>
#include "Config_Handler.h"
#include "LockFreeQueue.h"
#include "Client_Communication_Handler.h"
#include "Kafka_Communication_Handler.h"
#include "Kafka_Producer_Handler.h"
#include "LockFreeQueue.h"
#include "Data_Structure.h"
#include "Application_Activity_Logger.h"
//#include "temp_consumer.h"


class Server_Master_Controller: public QObject
{
    Q_OBJECT
public:
    explicit Server_Master_Controller(QObject *parent = nullptr);

    //creating global variable to access Config_Handler from anywhere in this module
    Config_Handler *Initialize_Config;

    //creating global variable to access Client_Communication_Handler from anywhere in this module
    Client_Communication_Handler *Initialize_Client_comm;

    //creating global variable to access Kafka_Producer_Handler from anywhere in this module
    Kafka_Producer_Handler *Initialize_kafkaProducer;

    //creating global variable to access Queue_Manager from anywhere in this module
    LockFreeQueue<QByteArray> *Initialize_queue_manager;

    //creating global variable to access kafka_communication_handler from anywhere in this module
    Kafka_Communication_Handler *Initialize_kafka_comm;

    //creating global variable to access Application_Activity_Logger from anywhere in this module
    Application_Activity_Logger *Log_data;

//    Temp_Consumer *consumer;

    QList<LockFreeQueue<QMap<int, DIGITAL_DATA>>*> DigitalQueueList;
    QList<LockFreeQueue<QMap<int,DataPacket>>*> AnalogQueueList;

    //to store all post segmentation queue
//    QMap<int, Queue_Manager<DataPacket>*> channelQueues;





signals:
    //Used to send trigger to kafka_communication_hanlder
    void sendDataToKafka(QJsonObject);
    //Used to send trigger to client_communication_hanler
    void sendDataToSocket(const STATUS& data);

public slots:
    //Used to get the trigger from kafka_communication_hanlder
    void HandleKafkaReceivedData(QJsonObject Command);
    //Used to get the trigger from client_communication_hanler
    void UpdateSocketReceivedData(int Command);
    //Used to get the trigger from config_handler
    void onConfigLoaded(QString data, int flag);


};

#endif // MASTER_CONTROLLER_H
