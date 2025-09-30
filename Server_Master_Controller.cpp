/**
  *********************************************************************************************************************************************
  * @file    Server_Master_Controller.cpp
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provide all kind control Command slots and  init modules.
  * @details 1.Initialize Config_Handler.
 *           2.Initialize Client_Communication_Handler.
 *           3.Initialize Kafka_Communication_Handler.
 *           4.Initialize Kafka_Producer_Handler.
 *           5.Initialize Queue_Manager.
 *           6.Upon receiving a signal from either the kafka_communication handler or the client_communication handler,
 *             the corresponding process is initiated.
 *           7.Emit signals to both the kafka_communication handler and the client_communication handler,
 *             to trigger the initialization of their respective processes.
  ********************************************************************************************************************************************/

/*
 *Header file which is included and used
 */
#include "Server_Master_Controller.h"


/**
 * @brief   Server_Master_Controller constructor to initialize Config_Handler modules and signal slot for that module
 * @param   parent
 * @return  None
 * @details Initialize Config_Handler module
 *          Initialize signal slot to receive sucess or failure status from the config-handler module.
 */
Server_Master_Controller::Server_Master_Controller(QObject *parent) : QObject(parent)
{
    //Default file path
    QString Server_filePath =QDir::currentPath()+"/DACS_Server_config.ini";
    QString Kafka_filePath = QDir::currentPath()+"/DACS_Kafka_config.ini";

    //get singleton object for config-handler module
    Initialize_Config = &Config_Handler::getInstance();

    //Initialize signal & slot to receive the success or failure Command from config_handler module to init all other modules
    if(!connect(Initialize_Config,SIGNAL(configLoaded(QString, int)),this,SLOT(onConfigLoaded(QString, int)))){
        if(Initialize_Config->getLogLevel()>=LOGLEVELINFO)
            activityLog<<"Config_Handler signal-slot is failed to init";
    }

    //Call init_Server function and pass the server filepath to read DACS_Server_config data from cofig file
    Initialize_Config->init_Server(Server_filePath);

    //Call init_Kafka function and pass the server filepath to read DACS_Kafka_config data from cofig file
    Initialize_Config->init_Kafka(Kafka_filePath);

}



/**
 * @brief   This function is used to init all module, once the config module is parsing sucessful all config data
 *          Otherwise it won't start all modules simply waits
 * @param   success or failure message as QString data
 * @param   success or failure flag as int flag
 * @return  None
 * @details Initialize Client_Communication_Handler module
 *          Initialize Kafka_Communication_Handler module
 *          Initialize Kafka_Producer_Handler module and start the threads
 *          Initialize signal slot to receive data from kafka_communication_handler module
 *          Initialize signal slot to send data to kafka_communication_handler
 *          Initialize signal slot to receive data from client_communication_handler module
 *          Initialize signal slot to send data to client_communication_handler
 */
void Server_Master_Controller::onConfigLoaded(QString data, int flag) {

    //check the flag is success or not
    //If the flag is 1, then initialize remaining modules
    //If the flag is 0, log the error message into the log file and exit.
    if(flag!=0){

        if(Initialize_Config->getLogLevel()>LOGLEVELINFO)
            activityLog<<"SIGNAL GOT EMIT BY CONFIG_HANDLER : " <<data;

        //Creating Client_Communication_Handler object to initiate that class
        Initialize_Client_comm = new Client_Communication_Handler();

        //Creating Kafka_Communication_Handler object to initiate that class
        Initialize_kafka_comm = new Kafka_Communication_Handler();

        //Creating Queue_Manager object to initiate that class
        Initialize_queue_manager = new LockFreeQueue<QByteArray>();

        //apply for linger.ms in producer configuration
        int refreshRate  =  Initialize_Config->getTestConfiguration().Refresh_Rate;

        //Get the test configuration map from config handler class
        QMap<int, int> ChannelID_SamplingRate = Initialize_Config->getTestConfiguration().ChannelID_SamplingRate;

        //local variable to to store samplingrate as key and list of channelID as value
        QMap<int, QList<int>> samplingRateGroups;

        // Group channelIDs by samplingRate
        for (auto it = ChannelID_SamplingRate.begin(); it != ChannelID_SamplingRate.end(); ++it) {
            int channelID = it.key();
            int samplingRate = it.value();

            samplingRateGroups[samplingRate].append(channelID);
        }


        //create map to store the list of channelID as key and value as bandwidthrange queue
        QMap<QList<int>, LockFreeQueue<QMap<int, DataPacket>>*> channelIDQueueMap;


        //Prepare the channelID list as key and relevant bandwidth range queue as value and store the map into above map for analog
        for (int i=1;i<=Initialize_Config->getKafkadetail().NoOfAnalogTopic;i++) {

            Analog_output_topic AnalogOutputTopic = Initialize_Config->getAnalogOutputTopicDetail().value(i);

            //create bandwidth range queue for analog
            LockFreeQueue<QMap<int, DataPacket>> *AnalogBandWidthRangeQ = new LockFreeQueue<QMap<int, DataPacket>>();

            for (auto it = samplingRateGroups.begin(); it != samplingRateGroups.end(); ++it) {
                int samplingRate = it.key();

                //                if(samplingRate == AnalogOutputTopic.maximum_sampling_rate || samplingRate == AnalogOutputTopic.minimum_sampling_rate){
                if (samplingRate >= AnalogOutputTopic.minimum_sampling_rate && samplingRate <= AnalogOutputTopic.maximum_sampling_rate){

                    QList<int> channelIDs = it.value();
                    channelIDQueueMap.insert(channelIDs, AnalogBandWidthRangeQ);
                }
            }

            qDebug()<<"onConfigLoaded channelIDQueueMap::"<<channelIDQueueMap;

            //create thread object
            QThread *producer_Thread = new QThread;

            //Creating Kafka_Producer_Handler object to initiate that class for analog topics
            Kafka_Producer_Handler *worker = new Kafka_Producer_Handler(AnalogOutputTopic.topicName, refreshRate,AnalogBandWidthRangeQ);

            //assign the object to the thread
            worker->moveToThread(producer_Thread);

            //Init signal slots for thread saftey
            connect(producer_Thread, &QThread::started, worker, &Kafka_Producer_Handler::kafka_Producer_Thread);
            connect(worker, &Kafka_Producer_Handler::finished, producer_Thread, &QThread::quit);
            connect(worker, &Kafka_Producer_Handler::finished, worker, &QObject::deleteLater);
            connect(producer_Thread, &QThread::finished, producer_Thread, &QObject::deleteLater);

            //start the thread
            producer_Thread->start();
        }


        //Prepare the channelID list as key and relevant bandwidth range queue as value and store the map into above map for Digital
        for (int i=1;i<=Initialize_Config->getKafkadetail().NoOfDigitalTopic;i++) {

            Digital_output_topic DigitalOutputTopic = Initialize_Config->getDigitalOutputTopicDetail().value(i);

            //create bandwidth range queue for digital
            LockFreeQueue<QMap<int, DataPacket>> *DigitalBandWidthRangeQ = new LockFreeQueue<QMap<int, DataPacket>>();

            for (auto it = samplingRateGroups.begin(); it != samplingRateGroups.end(); ++it) {
                int samplingRate = it.key();

                if(samplingRate == DigitalOutputTopic.sampling_rate){
                    QList<int> channelIDs = it.value();
                    channelIDQueueMap.insert(channelIDs, DigitalBandWidthRangeQ);
                }
            }

            //create thread object
            QThread *producer_Thread = new QThread;

            //Creating Kafka_Producer_Handler object to initiate that class for digital topics
            Kafka_Producer_Handler *worker = new Kafka_Producer_Handler(DigitalOutputTopic.topicName,refreshRate,DigitalBandWidthRangeQ);

            //assing the object to the thread
            worker->moveToThread(producer_Thread);

            //Init signal slots for thread saftey
            connect(producer_Thread, &QThread::started, worker, &Kafka_Producer_Handler::kafka_Producer_Thread);
            connect(worker, &Kafka_Producer_Handler::finished, producer_Thread, &QThread::quit);
            connect(worker, &Kafka_Producer_Handler::finished, worker, &QObject::deleteLater);
            connect(producer_Thread, &QThread::finished, producer_Thread, &QObject::deleteLater);

            //start the thread
            producer_Thread->start();
        }

        //set the map into config handler variable to access from data segmentation thread
        Initialize_Config->setChannelIDQueueMap(channelIDQueueMap);

        //        for (auto it = Initialize_Config->getChannelIDQueueMap().begin(); it != Initialize_Config->getChannelIDQueueMap().end(); ++it) {
        //            const QList<int>& channelIDs = it.key();
        //            Queue_Manager<QMap<int, DataPacket>>* queuePtr = it.value();

        //            // Print channel IDs
        //            qDebug() << "Channel IDs: ";
        //            for (int chID : channelIDs) {
        //                qDebug() << chID;
        //            }

        //            // Print queue pointer address (or you can add more info if Queue_Manager has a method)
        //            qDebug() << "Queue pointer:" << queuePtr;

        //            qDebug() << "---------------------------";
        //        }



        //Initialize signal slot to receive the data from kafka_communication_handler module
        if(!connect(Initialize_kafka_comm,SIGNAL(receivedDataFromKafka(QJsonObject)),this,SLOT(HandleKafkaReceivedData(QJsonObject))))
            activityLog<<"To receive Data From Kafka [receivedDataFromKafka] signal-slot is failed to init";

        //Initialize signal slot to send data to kafka_communication_handler module from this module
        if(!connect(this,SIGNAL(sendDataToKafka(QJsonObject)),Initialize_kafka_comm,SLOT(UpdateSendDataToKafka(QJsonObject))))
            activityLog<<"To send Data To Kafka [sendDataToKafka] signal-slot is failed to init";


        //Initialize signal slot to receive the data from kafka_communication_handler module
        if(!connect(Initialize_Client_comm,SIGNAL(receiveDataFromSocket(int)),this,SLOT(UpdateSocketReceivedData(int))))
            activityLog<<"To receive Data From Socket [receiveDataFromSocket] signal-slot is failed to init";

        //Initialize signal slot to send data to kafka_communication_handler module from this module
        if(!connect(this, SIGNAL(sendDataToSocket(STATUS)),Initialize_Client_comm, SLOT(sendDataToAllClients(STATUS))))
            activityLog<<"To send Data To Socket [sendDataToSocket] signal-slot is failed to init";



        //        QThread::msleep(1000);
        /******************************************TESTING PUROPSE************************************************/
        //                            QHostAddress IP = QHostAddress("192.168.2.109");
        //                            Client_Handler *client  = new Client_Handler(IP, 55668);
        /*********************************************************************************************************/

        //                            //create thread object
        //                            QThread *consumer_Thread = new QThread;

        //                            //Creating Kafka_Producer_Handler object to initiate that class for analog topics
        //                            Temp_Consumer *worker = new Temp_Consumer("TOPIC_A_500_1000","192.168.2.109:9092");

        //                            //assing the object to the thread
        //                            worker->moveToThread(consumer_Thread);

        //                            //Init signal slots for thread saftey
        //                            connect(consumer_Thread, &QThread::started, worker, &Temp_Consumer::startConsumer);
        //                            connect(worker, &Temp_Consumer::finished, consumer_Thread, &QThread::quit);
        //                            connect(worker, &Temp_Consumer::finished, worker, &QObject::deleteLater);
        //                            connect(consumer_Thread, &QThread::finished, consumer_Thread, &QObject::deleteLater);

        //                            //start the thread
        //                            consumer_Thread->start();

    }else {
        //log the error message into the log file
        activityLog<<data;
        return ;
    }
}


/**
 * @brief   This function is used to get data from kafka_communication_handler module
 * @param   Command is the data, which is get from the kafka communication topic
 * @return  None
 * @details If signal gets emitted from kafka_communication_handler this slot will get that data.
 *          Check the data to start and stop the ACQ, as of now.
 *          Emit the signal to client_communication_handler module to send Command to all clients.
 */
void Server_Master_Controller::HandleKafkaReceivedData(QJsonObject Command){

    QJsonObject received_json = Command;
    int key = received_json["key"].toInt();
    quint8 acq = received_json["acquisitionFlag"].toInt();
    QString command = received_json["command"].toString();

    switch (key) {

    //key 1000 is for acq start and stop
    case 1000:
        qDebug()<<"entered case key is "<<key;
        if(acq==1){
            STATUS sendtoClient;
            sendtoClient.startAcq = 1;
            qRegisterMetaType<STATUS>("STATUS");
            //Emit the signal to send Command to the client socket
            emit sendDataToSocket(sendtoClient);
//            Initialize_Config->getInstance().setAcqFlag(1);
            QJsonObject sendData;
            sendData["key"]=1001;
            sendData["startAcq"]=1;
            sendData["command"]="startAcq command is received";
            emit sendDataToKafka(sendData);
        }else if (acq==0) {
            STATUS sendtoClient;
            sendtoClient.startAcq =acq;
            qRegisterMetaType<STATUS>("STATUS");
            //Emit the signal to send Command to the client socket
            emit sendDataToSocket(sendtoClient);
            Initialize_Config->getInstance().setAcqFlag(0);
        }
        break;

        //key 2000 is for readyfor acq and not readyfor acq
    case 2001:
        qDebug()<<"entered case key is "<<key;
        qDebug()<<"received acknowledge from kafka command "<<command;
        activityLog<<"received ack for readyForAcq from kafka "<<command;
        break;

        //key 2000 is for not readyfor acq and not readyfor acq
    case 3001:
        qDebug()<<"entered case key is "<<key;
        qDebug()<<"received acknowledge from kafka command "<<command;
        activityLog<<"received ack for not readyForAcq from kafka "<<command;
        break;

    }
}


/**
 * @brief   This function is used to get data from client_communication_handler module
 * @param   Command is the data, which is get from the client_communication_handler
 * @return  None
 * @details If signal gets emitted from client_communication_handler this slot will get that data.
 *          Check the data if the client is ready to start for the Acq or not ready.
 *          Emit the signal to kafka_communication_handler to send Command to the kafka server.
 */
void Server_Master_Controller::UpdateSocketReceivedData(int Command){

    qDebug()<<Command <<"all client is ready for acq flag received from client communication handler";
    if(Command==1){
        QJsonObject json;
        json["key"] = 2000;
        json["readyForAcq"] = Command;
        json["command"] = "Server and Clients are setup is ready waiting for Start ACQ";
        //Emit the signal to send Command to the kafka server
        emit sendDataToKafka(json);
        //testing purpose to send startAcq to clients
        STATUS data;
        data.startAcq=1;
        emit sendDataToSocket(data);
    }else if(Command == 0){
        QJsonObject json;
        json["key"] = 2000;
        json["readyForAcq"] = Command;
        json["command"] = "DAQ client setup was failure cannot start the ACQ / DACS client or server setup is not ready cannot start the ACQ";
        //Emit the signal to send Command to the kafka server
        emit sendDataToKafka(json);
    }else if(Command == 2){
        QJsonObject json;
        json["key"] = 3000;
        json["readyForAcq"] = Command;
        json["command"] = "cannot start the tcp server for Initial communication with clients don't start the ACQ";
        //Emit the signal to send Command to the kafka server
        emit sendDataToKafka(json);
    }
}


