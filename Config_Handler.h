/**
*****************************************************************************************************************************************
* @file    Config_Handler.h
* @author  karthikeya.sr
* @version V1.0
* @date    02-May-2025
* @brief   This file provides all headers to Config_Handler.cpp
***************************************************************************************************************************************/



#ifndef CONFIG_HANDLER_H
#define CONFIG_HANDLER_H

#define LOGLEVELINFO 1


/*
 *Header file which is included and used
 */
#include <QObject>
#include <QMap>
#include <QSettings>
#include <QDir>
#include <QDebug>
#include <QThread>
#include <QtConcurrent>
#include "Data_Structure.h"
#include "LockFreeQueue.h"
#include "Application_Activity_Logger.h"


class Config_Handler: public QObject
{
    Q_OBJECT
public:
    explicit Config_Handler(QObject *parent = nullptr);

    //single-ton instance for Config_Handler class
    static  Config_Handler& getInstance(){
        static Config_Handler config_handler_instance;
        return config_handler_instance;
    }

    int init_Server(QString server_filePath) ;
    int init_Kafka(QString Kafka_filePath);
    /*****************************Server Configuration Variables*********************/

    //Setter for ServerConfiguration
    void setSERVERdetail(const SERVER &VALUE) {
        server = VALUE;
    }

    //Getter for ServerConfiguration
    const SERVER& getSERVERdetail() const {
        return server;
    }

    /***********************************Test Configuration Variables************************************************/


    //Setter for TestConfiguration
    void setTestConfiguration(const TEST &value){
        test_config = value;
    }

    //Getter for TestConfiguration
    const TEST &getTestConfiguration() const {
        return test_config;
    }



    /***********************************Logger information************************************************/

    //Getter for Log_Level
    int getLogLevel() const {
        return Log_Level;
    }
    //Setter for Log_Level
    void setLogLevel(int value) {
        Log_Level = value;
    }

    /***********************************Kafka Configuration Variables************************************************/

    //Getter for kafkaConfiguration
    KAFKA getKafkadetail() const {
        return kafka;
    }

    //Setter for kafkaConfiguration
    void setKafkadetail(const KAFKA &value) {
        kafka = value;
    }

    /***********************************Communication Topic Configuration Variables************************************************/

    //Getter for kafka COMMUNICATION Configuration
    COMMUNICATION_TOPIC getCommunicationdetail() const {
        return comm_topic;
    }

    //Setter for kafka COMMUNICATION Configuration
    void setCommunicationdetail(const COMMUNICATION_TOPIC &value) {
        comm_topic = value;
    }

    /*********************************** Analog and Digital Topic Variables************************************************/


    //Getter for AnalogOutputTopicDetail
    const QMap<int, Analog_output_topic>& getAnalogOutputTopicDetail() const {
        return Analog_Output_topic_detail;
    }

    //Setter for AnalogOutputTopicDetail
    void setAnalogOutputTopicDetail(const QMap<int, Analog_output_topic> &value) {
        Analog_Output_topic_detail = value;
    }

    //Getter for DigitalOutputTopicDetail
    const QMap<int, Digital_output_topic>& getDigitalOutputTopicDetail() const {
        return Digital_Output_topic_detail;
    }

    //Setter for AnalogOutputTopicDetail
    void setDigitalOutputTopicDetail(const QMap<int, Digital_output_topic> &value) {
        Digital_Output_topic_detail = value;
    }

    // Getter for channelID list and relavant bandwidth range queue
    const QMap<QList<int>, LockFreeQueue<QMap<int, DataPacket>>*>& getChannelIDQueueMap() const {
        return channelIDQueueMap;
    }

    // Setter for channelID list and relavant bandwidth range queue
    void setChannelIDQueueMap(const QMap<QList<int>, LockFreeQueue<QMap<int, DataPacket>>*>& newMap) {
        channelIDQueueMap = newMap;
    }


    // Wait condition and mutex to control thread execution
    QWaitCondition acqCondition;
    QMutex acqMutex;


    //Getter for ACQFLAG
    int getAcqFlag() const {
        return AcqFlag;
    }


    //Setter for ACQFLAG
    void setAcqFlag(int value) {
        AcqFlag = value;
        QMutexLocker locker(&acqMutex);
                AcqFlag = value;

                if (AcqFlag == 1) {
                    acqCondition.wakeAll(); // Wake all waiting threads
                }
    }






public slots:
    //Used to read the server related configurations.
    int Read_Server_Log_Config(QSettings *object);

    int Read_Test_config(QSettings *object);

    //Used to read the kafka related configurations.
    int Read_Kafka_Config(QSettings *object);

    //Used to read communication topic configurations.
    int Read_Communication_Config(QSettings *object);

    //Used to read Analog topic configurations.
    int Read_Analog_Topic_Config(QSettings *object);

    //Used to read Digital topic configurations.
    int Read_Digital_Topic_Config(QSettings *object);


public:
    int AcqFlag;


signals:
    //Used to send trigger to server_master_controller
    void configLoaded(QString data, int flag);

private:

    //Server configuration variables
    SERVER server;

    //kafka configuration variables
    KAFKA kafka;

    //Communication Topic configuration
    COMMUNICATION_TOPIC comm_topic;

    //Analog and digital topic variables
    QMap<int, Analog_output_topic> Analog_Output_topic_detail; // key and object with topic name & max,min of sampleRate for each analog topic.
    QMap<int, Digital_output_topic> Digital_Output_topic_detail;
    QList<QList<int>> GroupOfChannels; //used to store channelID's list based on the bandwidth range

    //test configuration variables
    TEST test_config;

    //Logger information
    int Log_Level;

    //channelId's and queue map
    QMap<QList<int>, LockFreeQueue<QMap<int, DataPacket>>*> channelIDQueueMap;





};

#endif // CONFIG_HANDLER_H
