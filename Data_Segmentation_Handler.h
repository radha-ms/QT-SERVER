/**
  ***************************************************************************************************************************************
  * @file    Data_Segmentation_Handler.h
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all headers to Data_Segmentation_Handler.cpp
  ********************************************************************************************************************************************/

#ifndef DATA_SEGMENTATION_HANDLER_H
#define DATA_SEGMENTATION_HANDLER_H

/*
 * Header files
 */
#include <QtConcurrent>
#include "LockFreeQueue.h"               // ✅ Use your custom lock-free queue
#include "Data_Structure.h"
#include "Config_Handler.h"

class Data_Segmentation_Handler : public QObject {
    Q_OBJECT

public:
    Data_Segmentation_Handler(
        LockFreeQueue<QByteArray> *queue,
        QMap<QList<int>, LockFreeQueue<QMap<int, DataPacket>>*> map,
        QObject *parent = nullptr
    );

    void Data_Segmentation_Thread();

    Config_Handler *config;

signals:
    void finished();

private:
    LockFreeQueue<QByteArray> *pre_SegmentationQueue;  // ✅ lock-free version
    QMap<QList<int>, LockFreeQueue<QMap<int, DataPacket>>*> channelIDRangeMap;
    QByteArray buffer;
};

#endif // DATA_SEGMENTATION_HANDLER_H




///**
//  ***************************************************************************************************************************************
//  * @file    Data_Segmentation_Handler.h
//  * @author  karthikeya.sr
//  * @version V1.0
//  * @date    02-May-2025
//  * @brief   This file provides all headers to Data_Segmentation_Handler.cpp
//  ********************************************************************************************************************************************/



//#ifndef DATA_SEGMENTATION_HANDLER_H
//#define DATA_SEGMENTATION_HANDLER_H

///*
// *Header file which is included and used
// */
//#include <QtConcurrent>
//#include "Queue_Manager.h"
//#include "Data_Structure.h"
//#include "Config_Handler.h"

//class Data_Segmentation_Handler :public QObject {
//    Q_OBJECT
//public:
//    Data_Segmentation_Handler(Queue_Manager<QByteArray> *queue, QMap<QList<int>, Queue_Manager<QMap<int, DataPacket>>*> map , QObject *parent =nullptr);

//    void Data_Segmentation_Thread();

//    Config_Handler *config;

//signals:
//    void finished();

//private:
////    QSharedPointer<ThreadSafeQueue<QList<ANALOG_DATA>>> pre_segmentQueue;

//Queue_Manager<QByteArray> *pre_SegmentationQueue;
//QMap<QList<int>, Queue_Manager<QMap<int, DataPacket>>*> channelIDRangeMap;
//QByteArray buffer;

//};

//#endif // DATA_SEGMENTATION_HANDLER_H
