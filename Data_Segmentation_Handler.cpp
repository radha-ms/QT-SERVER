/**
  ***************************************************************************************************************************************
  * @file    Data_Segmentation_Handler.cpp
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all information about the seggregate the incoming data and push it to relevant bandwidth queue.
  *
  * @details 1.Listening for pre_segmentation_queue data.
  *          2.Once data arrives, extract the channelID.
  *          3.Compare with the test detail.
  *          4.Put into appropriate bandwidth range queue.
  ******************************************************************************************************************************************/



/*
 *Header file which is included and used
 */
#include "Data_Segmentation_Handler.h"


/**
 * @brief Data_Segmentation_Handler::Data_Segmentation_Handler constructor
 * @param None
 * @return None
 * @details Spawn Data_Segmentation_Thread
 */
//Data_Segmentation_Handler::Data_Segmentation_Handler(LockFreeQueue<QByteArray> *queue,QMap<QList<int>, LockFreeQueue()<QMap<int, DataPacket>>*> map , QObject *parent)
//    : QObject (parent), pre_SegmentationQueue(queue), channelIDRangeMap(map){}

Data_Segmentation_Handler::Data_Segmentation_Handler(LockFreeQueue<QByteArray> *queue,
                                                     QMap<QList<int>, LockFreeQueue<QMap<int, DataPacket>>*> map,
                                                     QObject *parent)
    : QObject(parent), pre_SegmentationQueue(queue), channelIDRangeMap(map) {}
/**
 * @brief   Data_Segmentation_Handler::Data_Segmentation_Thread
 * @param   None
 * @return  None
 * @details Listens data from pre_segmentation queue
 *          Read and store all received bytes into the buffer
 *          Take the first fixed size of bytes from the buffer and deserialize the channelType and check A or D
 *          Based on the channelType, Deserialize the bytearray as a Analog struct or Digital struct
 *          And then with the help of Map choose the relevant bandwidthRange queue to send the data
 *          Create one map to store the channelId as a key and the struct as a value
 *          Then send the map into the choosen bandwidth range queue.
 *          Once the fixed sixe of byte is processed, then remove the bytes from the buffer to take the next set of bytes
 */
void Data_Segmentation_Handler::Data_Segmentation_Thread(){

    //check the map is not empty
    //        if (channelIDRangeMap.isEmpty()) {
    //            activityLog << "channelIDRangeMap is empty in data segmentation thread, cannot start the threads for segment!";
    //    //        exit(0);
    //        } else {
    //            if(config->getLogLevel()>LOGLEVELINFO)
    //                activityLog << "channelIDRangeMap has entries so ready to spawn segment thread";
    //        }

    //const size of both analog and digital data packet size
    constexpr int PACKET_SIZE = 81;

    qDebug() << "Data segmentation thread is waiting for AcqFlag...";

    // Wait until AcqFlag is 1
    //          {
    //              QMutexLocker locker(&config->getInstance().acqMutex);
    //              while (config->getInstance().getAcqFlag() == 0) {
    //                  config->getInstance().acqCondition.wait(&config->getInstance().acqMutex);
    //              }
    //          }

    qDebug() << "Data segmentation thread is received start acqFlag...";

    while (true) {

        // Check periodically if AcqFlag becomes 0 to stop
        //               if (config->getInstance().getAcqFlag() == 0) {
        //                   qDebug() << "AcqFlag became 0, Data segment waiting again...";
        //                   QMutexLocker locker(&config->getInstance().acqMutex);
        //                   while (config->getInstance().getAcqFlag() == 0) {
        //                       config->getInstance().acqCondition.wait(&config->getInstance().acqMutex);
        //                   }
        //                   qDebug() << "Data segmentation thread resumed after AcqFlag set to 1.";
        //                   continue;
        //               }

        //read data from the pre-segmentation queue which is enqueued from data receiver thread
        //    QByteArray buffer = pre_SegmentationQueue->deQueue();
        QByteArray buffer;
        if (pre_SegmentationQueue->deQueue(buffer)) {
            // Use buffer


            //        qDebug()<<"data segmentation thread received bytes "<<buffer.size();
            //   qDebug()<<"buffer data::"<<buffer.size();

            //enter if the buffer size if more than one packet size
            while (buffer.size() >= PACKET_SIZE) {


                //read only  one PACKET_SIZE from the whole buffer
                QByteArray packet = buffer.left(PACKET_SIZE);
                //            qDebug()<<"data segmentation entered packet bytes "<<packet.size();
                QDataStream in(&packet, QIODevice::ReadOnly);
                in.setByteOrder(QDataStream::LittleEndian);

                //deserialize channelType
                qint8 channelType;
                in >> channelType;

                //casting channelType
                QChar c((short) channelType);

                //check the channelType is analog or digital
                if (c == 'A') {
                    ANALOG_DATA analog;

                    if (packet.size() == sizeof(ANALOG_DATA)) {
                        memcpy(&analog, packet.constData(), sizeof(ANALOG_DATA));

//                        qDebug() << "CHANNEL ID in seg:"<<analog.channelID;
                        //            qDebug() << "[Data segmentation] ANALOG_DATA deserialized successfully"<<analog.channelID;


                    } else {
                        qWarning() << "[Data segmentation] Incomplete ANALOG_DATA received. Expected"
                                   << sizeof(ANALOG_DATA) << "bytes, but got" << packet.size();
                    }


                    //check the channelID is present in the map keys
                    for (const QList<int>& keyList : channelIDRangeMap.keys()) {


                        //enter if the channelID is present in the map keylist
                        if (keyList.contains(analog.channelID)) {

                            //take the relevant queue for the channelID which is stored in value for the key channelID
                            LockFreeQueue<QMap<int, DataPacket>>* bandWidthRangeQueue = channelIDRangeMap.value(keyList);


                            //check the queue is present or not
                            if (bandWidthRangeQueue) {

                                // Prepare the QMap<int, DataPacket> with analog.channelID and analog object
                                QMap<int, DataPacket> dataMap;

                                //analog packet is assigned to DataPacket and insert into datamap
                                dataMap.insert(analog.channelID, analog);

                                // Now push dataMap to the bandWidthRangeQueue
                                bandWidthRangeQueue->enQueue(dataMap);

//                                qDebug()<<"analog send to kafka from sgment "<<analog.channelID <<bandWidthRangeQueue;

                            } else {
                                qDebug() << "Queue manager pointer is null!";
                            }
                            break;  // stop once found
                        }
                    }
                }

                //check the channelType is digital or not
                else if (c == 'D') {
                    DIGITAL_DATA digital;

                    if (packet.size() == sizeof(DIGITAL_DATA)) {
                        memcpy(&digital, packet.constData(), sizeof(DIGITAL_DATA));

                        qDebug() << "[Data segmentation] DigitalData deserialized successfully";
                    } else {
                        qWarning() << "[Data segmentation] Incomplete DIGITAL_DATA received. Expected"
                                   << sizeof(DIGITAL_DATA) << "bytes, but got" << packet.size();
                    }

                    //check the cardID is present in the map keys
                    for (const QList<int>& keyList : channelIDRangeMap.keys()) {

                        //enter if the channelID is present in the map keylist
                        if (keyList.contains(digital.cardID)) {

                            //take the relevant queue for the channelID which is stored in value for the key cardID
                            LockFreeQueue<QMap<int, DataPacket>>* bandWidthRangeQueue = channelIDRangeMap.value(keyList);

                            //check the queue is present or not
                            if (bandWidthRangeQueue) {

                                // Prepare the QMap<int, DataPacket> with digital.cardID and digital object
                                QMap<int, DataPacket> dataMap;

                                // digital packet is assigned to DataPacket and insert into the datamap
                                dataMap.insert(digital.cardID, digital);

                                // Now push dataMap to the bandWidthRangeQueue
                                bandWidthRangeQueue->enQueue(dataMap);
                                qDebug()<<"Digital sent to kafka from segment "<<digital.cardID<<bandWidthRangeQueue;
                            } else {
                                qDebug() << "Queue manager pointer is null!";
                            }
                            break;  // stop once found
                        }
                    }

                }
                else {
                    qDebug() << "Unknown packet type at start of buffer:" << channelType;
                    break;  // Exit loop on unknown data
                }

                //remove the only readed data from the buffer
                buffer.remove(0, PACKET_SIZE);
                //                qDebug() << "Remaining buffer size:" << buffer.size();
            }QThread::msleep(1);
        }

    }

}

