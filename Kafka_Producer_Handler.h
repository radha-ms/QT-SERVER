/**
  ***************************************************************************************************************************************
  * @file    Kafka_Producer_Handler.h
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all headers to Kafka_Producer_Handler.cpp
  ********************************************************************************************************************************************/



#ifndef KAFKA_PRODUCER_HANDLER_H
#define KAFKA_PRODUCER_HANDLER_H

/*
 *Header file which is included and used
 */
#include <QObject>
#include <QDebug>
#include <QtConcurrent>
#include <Config_Handler.h>
#include "LockFreeQueue.h"
#include <librdkafka/rdkafkacpp.h>
#include "sensor_data.pb.h"
#include <arpa/inet.h>


using namespace std;
class Kafka_Producer_Handler: public QObject
{
    Q_OBJECT
public:
    explicit Kafka_Producer_Handler(QString topicname, int refreshRate,  LockFreeQueue<QMap<int, DataPacket>> *bandWidthQueue,QObject *parent = nullptr);
    void kafka_Producer_Thread();
    int create_Producer_Instance();

    QList<LockFreeQueue<DIGITAL_DATA>*> DigitalQueueList;
    QList<LockFreeQueue<ANALOG_DATA>*> AnalogQueueList;
    Config_Handler *config;

signals:
    void finished();

private:
    LockFreeQueue<QMap<int, DataPacket>> *dataQueue;
    int sendingFrequency;
    QString TopicNametoSend;
    string topicnameREAL;


    RdKafka::Producer* producer;
    string producer_broker,producer_errstr;
    string serializer,DeSerializer;

};

#endif // KAFKA_PRODUCER_HANDLER_H
