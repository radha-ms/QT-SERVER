/**
  ***************************************************************************************************************************************
  * @file    Client_Communication_Handler.h
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all headers to Client_Communication_Handler.cpp
  ********************************************************************************************************************************************/


#ifndef COMMUNICATION_HANDLER_H
#define COMMUNICATION_HANDLER_H

/*
 *Header file which is included and used
 */
#include <QtConcurrent>
#include <QObject>
#include <QTcpServer>
#include <QTcpSocket>
#include "Client_Handler.h"
#include "Data_Structure.h"
#include "Config_Handler.h"
#include "LockFreeQueue.h"
#include <QTcpServer>
#include <QList>
#include <QTcpSocket>
#include <QByteArray>
#include <QDataStream>
#include <QString>

class Client_Communication_Handler : public QTcpServer
{
    Q_OBJECT

public:
    explicit Client_Communication_Handler(QObject *parent = nullptr);
    //used to listen the tcp server in provided ip and port
    int startServer();

    //Used to get new port number from system
    quint16 getAvailablePort();



private slots:
    //used to accept the new client
    void newConnection();
    //used to delete the client socket when the client disconnects
    void clientDisconnected();
    //used to read data from all clients
    void readClientData();
    //Used to get the trigger from server_master_controller module
    void UpdateSocketSendData(const STATUS& Command);
    //used to send data as struct to all connected clients
    void sendDataToAllClients(STATUS data);



private:
    QList<QTcpSocket*> m_clients;
    QHostAddress serverIP;
    quint32 serverPort;
    QTcpSocket *m_socket;

    struct client{
        QString controllerID;
        quint32 dedicatedPort;
        QTcpSocket *tcpSocket;
        int readyForAcq;
    };
    bool allReady = false;



    //Used to store all client's tcp socket address
    QList<QTcpSocket *> connectedClients;
    QMap<QString, client> clients_details;


    Config_Handler *config;


signals:
    //Used to send trigger to server_master_controller module
    void receiveDataFromSocket(int Command);
};

#endif // COMMUNICATION_HANDLER_H
