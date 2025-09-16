/**
  ***************************************************************************************************************************************
  * @file    Data_Receiver_Handler.h
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all headers to Data_Receiver_Handler.cpp
  ********************************************************************************************************************************************/



#ifndef DATA_RECEIVER_HANDLER_H
#define DATA_RECEIVER_HANDLER_H

/*
 *Header file which is included and used
 */
#include <QtConcurrent>
#include <QObject>
#include <QThread>
#include <QTcpServer>
#include <QDataStream>
#include <QByteArray>
#include <QTcpSocket>
#include "LockFreeQueue.h"
#include "Data_Structure.h"
#include "Application_Activity_Logger.h"

class Data_Receiver_Handler : public QObject{
    Q_OBJECT
public:
    Data_Receiver_Handler(QHostAddress Ip, quint32 port, LockFreeQueue<QByteArray> *queue, QObject *parent =nullptr);

    void Data_Receiver_Thread();
    void dataGenerate1ms();
    void dataGenerate100ms();
    void dataGenerate1sec();
    void dataGenerate10ms();
    void dataGenerate50ms();

    int PACKET_SIZE = 81;

    // Simulate generating packets
//    QList<QByteArray> generatedPackets;
    QMutex mutex;
//    QByteArray buffer;
      int index = 0;

signals:
    void finished();

private:
    QHostAddress DedicatedIp;
    quint32 DedicatedPort;
    bool m_running;
    LockFreeQueue<QByteArray> *pre_SegmentationQueue;

};

#endif // DATA_RECEIVER_HANDLER_H
