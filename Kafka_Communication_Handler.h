/**
  ***************************************************************************************************************************************
  * @file    Kafka_Communication_Handler.h
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all headers to Kafka_Communication_Handler.cpp
  ********************************************************************************************************************************************/



#ifndef KAFKA_COMMUNICATION_HANDLER_H
#define KAFKA_COMMUNICATION_HANDLER_H
/*
 *Header file which is included and used
 */
#include <QObject>
#include <QtConcurrent>
#include <librdkafka/rdkafkacpp.h>
#include <stdlib.h>
#include <iostream>

#include "Config_Handler.h"
//#include "sensor_data.pb.h"
//#include <arpa/inet.h> // For ntohl

using namespace std;
class Kafka_Communication_Handler: public QObject
{
    Q_OBJECT
public:
    explicit Kafka_Communication_Handler(QObject *parent = nullptr);

    int create_Producer_Instance();
    int create_Consumer_Instance();
    void kafka_Listener_Thread();
    bool getDetails();
//    void kafka_consumer_Thread();

    //test case function
    int create_Invalid_Consumer_Instance();
    int create_Invalid_Producer_Instance();

public slots:
    void UpdateSendDataToKafka(QJsonObject data);

signals:
    void receivedDataFromKafka(QJsonObject);

private:
    Config_Handler *config;

    RdKafka::Topic *Topic;
    RdKafka::Consumer *consumer;
    string consumer_broker,consumer_errstr;

    RdKafka::Producer* producer;
    string producer_broker,producer_errstr;
    string serializer,DeSerializer;

    string COMM_TOPIC;

};

#endif // KAFKA_COMMUNICATION_HANDLER_H
