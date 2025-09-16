/**
  ***************************************************************************************************************************************
  * @file    Client_Handler.cpp
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all information about the creation of Data-Receiver_Handler and Data_Segmentation_Handler.
  *
  * @details 1.Get Initial IP and port from client_communication_handler
  *          2.Spawns Data_Receiver_Handler and Data_Segmentation_Handler thread with respective queue and IP & Port.
  *          3.Send the result success '1' or failure '0' to the client_communication_Handler.
  *
  ******************************************************************************************************************************************/



/*
 *Header file which is included and used
 */
#include "Client_Handler.h"


/**
 * @brief   Client_Handler constructor is used to init the threads
 * @param   dedicatedIP
 * @param   dedicatedPort
 * @return  None
 * @details call spawn_Thread function and pass the IP and Port to the function
 */
Client_Handler::Client_Handler(QHostAddress IP, quint32 Port, QObject *parent) : QObject(parent)
{

    //calling function to spawn threads
  //  IP = QHostAddress("192.168.2.108");  // ✅ correct
    int value = spawn_Threads( IP,  Port);
    if(value!=1){
        activityLog<<"error in spawn threads "<<value;
        qDebug()<<"error in spawn threads "<<value;
    }else {
        activityLog<<"spawn thread is success "<<value;
        qDebug()<<"spawn thread is success "<<value;
    }

}

/**
 * @brief   Client_Handler::spawn_Threads
 * @param   IP
 * @param   Port
 * @return  1 or 0 success or failure
 * @details Create a pre_SegmentationQueue object to store the rawdata
 *          Initialize and create a thread object for Data_Receiver_Handler and Data_Segmentation_Handler
 *          And pass the required IP,Port & pre_SegmentationQueue to the Data_Receiver_Handler
 *          And pass the pre_SegmentationQueue and MAP which is stored the channelID and appropriate
 *          BandwidthRange queue from config_Handler to the Data_Segmentation_Handler
 *          Start the threads
 */
int Client_Handler::spawn_Threads(QHostAddress IP, quint32 Port){

    qDebug()<<"spawn threads";

    //create the rawdata queue to store the bytearray which is send from client application.
    LockFreeQueue<QByteArray> *pre_SegmentationQueue = new LockFreeQueue<QByteArray>();

    //Get the map which has the listof channelID and relevant queue from config handler module
    QMap<QList<int>, LockFreeQueue<QMap<int, DataPacket>>*> channelID_Queue_Map = config->getInstance().getChannelIDQueueMap();
 //   qDebug()<<"channelID_Queue_Map::"<<channelID_Queue_Map;

//    check that map is not empty
//    if (channelID_Queue_Map.size()<=0) {
//        activityLog << "channelID_Queue_Map is empty in data receiver thread, cannot start the threads for client!";
////        return 0;
//    } else {
//        if(config->getLogLevel()>LOGLEVELINFO)
//            activityLog << "channelID_Queue_Map has entries so ready to spawn clients thread";
//    }

            for (auto it = channelID_Queue_Map.begin(); it != channelID_Queue_Map.end(); ++it) {
                const QList<int>& channelIDs = it.key();
                LockFreeQueue<QMap<int, DataPacket>>* queuePtr = it.value();

                // Print channel IDs
              //  qDebug() << "Channel IDs IN Data Segmentation Queue: ";
                for (int chID : channelIDs) {
                    qDebug() << chID;
                }

                // Print queue pointer address (or you can add more info if Queue_Manager has a method)
                qDebug() << "Queue pointer:" << queuePtr;

                qDebug() << "---------------------------";
            }

    //convert hostaddress to string format for checking purpose
    QString ipStr = IP.toString();
    qDebug()<<"ipStr               "<<ipStr;
    // Regular expression for strict IPv4 format validation
    QRegularExpression ipv4Strict(R"(^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$)");

    //check that ip add is valid or not
    if (ipStr.isEmpty() || !ipv4Strict.match(ipStr).hasMatch()) {
        activityLog << "In client handler received IP address is invalid (bad format)";
        return 0;
    }

    // If passed format check, then try QHostAddress
    QHostAddress addr(ipStr);
    if (addr.isNull()) {
        activityLog << "In client handler received IP address is null";
        return 0;
    }

    QString Dport = QString::number(Port);
    // Check if value is a valid integer (not float or non-numeric)
    bool isInt1 = false;
    Dport.toInt(&isInt1);
    if (!isInt1) {
        activityLog << "In client handler received tcp socket port for client: dedicatedPort must be an integer value, actual: " << Port;
        return 0;
    }

    // check Detect clearly invalid types like alphabets-only strings
    if (Dport.contains(QRegularExpression("^[a-zA-Z\\s]+$"))) {
        activityLog << "In client handler received tcp socket port for client: dedicatedPort appears to be a non-numeric string: " << Port;
        return 0;
    }

    //check that port is in valid Range
    if(Port <= 0  || Port>65535){
        activityLog<<"In client handler received tcp socket port for client because port is Invalid";
        return 0;
    }


    // data receiver Thread Setup
    QThread *receiver_Thread = new QThread;
    //initialize the data receiver handler and passing IP and Port and rawdata queue.
    Data_Receiver_Handler *worker = new Data_Receiver_Handler(IP,Port, pre_SegmentationQueue);
    worker->moveToThread(receiver_Thread);

    //initialize signal slot assinging the thread function to start and quit and delete
    connect(receiver_Thread, &QThread::started, worker, &Data_Receiver_Handler::Data_Receiver_Thread);
    connect(worker, &Data_Receiver_Handler::finished, receiver_Thread, &QThread::quit);
    connect(worker, &Data_Receiver_Handler::finished, worker, &QObject::deleteLater);
    connect(receiver_Thread, &QThread::finished, receiver_Thread, &QObject::deleteLater);
    //start the thread
    receiver_Thread->start();

    // segment Thread Setup
    QThread *segment_Thread = new QThread;
    //initialize the data segmentation handler and passing rawData queue,
    //And Map which is used to store the list of channelId as key, relevant queue as value
    Data_Segmentation_Handler *data_Segmentation = new Data_Segmentation_Handler(pre_SegmentationQueue, channelID_Queue_Map);
    data_Segmentation->moveToThread(segment_Thread);

    //initialize signal slot assinging the thread function to start and quit and delete
    connect(segment_Thread, &QThread::started, data_Segmentation, &Data_Segmentation_Handler::Data_Segmentation_Thread);
    connect(data_Segmentation, &Data_Segmentation_Handler::finished, segment_Thread, &QThread::quit);
    connect(data_Segmentation, &Data_Segmentation_Handler::finished, data_Segmentation, &QObject::deleteLater);
    connect(segment_Thread, &QThread::finished, receiver_Thread, &QObject::deleteLater);
    //start the thread
    segment_Thread->start();

    return 1;
}
