/**
*****************************************************************************************************************************************
* @file    Client_Handler.h
* @author  karthikeya.sr
* @version V1.0
* @date    02-May-2025
* @brief   This file provides all headers to Client_Handler.cpp
***************************************************************************************************************************************/



#ifndef CLIENT_HANDLER_H
#define CLIENT_HANDLER_H
/*
 *Header file which is included and used
 */
#include <QObject>
#include <QThread>
#include <QtConcurrent>
#include <Data_Receiver_Handler.h>
#include <Data_Segmentation_Handler.h>
#include <variant>
#include "LockFreeQueue.h"
#include "Data_Structure.h"
#include "Data_Receiver_Handler.h"
#include "Data_Segmentation_Handler.h"
#include "Config_Handler.h"

using namespace std;

class Client_Handler :public QObject
{
    Q_OBJECT
public:
    Client_Handler(QHostAddress IP, quint32 Port, QObject *parent = nullptr);

    //Used to create queue and spawn data receiver and data segmentation threads and pass relevant parameters
    int spawn_Threads(QHostAddress IP, quint32 Port);


private:
    Config_Handler *config;



};

#endif // CLIENT_HANDLER_H
