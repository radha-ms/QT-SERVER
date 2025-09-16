/**
  ***************************************************************************************************************************************
  * @file    Client_Communication_Handler.cpp
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all information about the communication process between DAQ server and DAQ clients
  *          and initializing Client_Handler class.
  *
  * @details 1.Get Initial IP and port from config_handler instance
  *          2.Starts the thread to communicate with the clients using the InitialIP and InitialPort
  *          3.Create tcp server to connect with clients, waits for the new connection.
  *          4.If any clients connected with the server, Initialize client_handler class and pass the client's Information.
  *          5.Upon receiving a signal from master_controller, sends to the DAQ clients.
  *          6.When receive a status command from clients, emits the signal to send the command to server_master-controller module.
  ******************************************************************************************************************************************/


/*
 *Header file which is included and used
 */
#include "Client_Communication_Handler.h"
#include "Config_Handler.h"

#include <QDebug>
#include <QtConcurrent>


/**
 * @brief   Client_Communication_Handler::Client_Communication_Handler constructor
 * @param   Parent
 * @return  None
 * @details Get the config handler instance
 *          Get the serverIp and serverPort from config handler and assing to global variables
 *          Initialize signal slot for accept the new client connection
 *          Call startServer function to wait for connection
 */
Client_Communication_Handler::Client_Communication_Handler(QObject *parent) : QTcpServer(parent) {

    //Get the Instance of config handler
    config = &Config_Handler::getInstance();

    //create a signal slot to accept the new client connection
    if(!connect(this, &QTcpServer::newConnection, this, &Client_Communication_Handler::newConnection))
        activityLog<<"failed to create signal slot for client connection";

    //    call function to listen in the port number to connect clients
    int result =  startServer();

    //check if start server is failed
    if(!result){

        //emit the signal as failed to start the tcp server
        emit receiveDataFromSocket(2);
    }
}

/**
 * @brief   Starts a TCP server on the specified port
 * @param   Port number
 * @return  '1' successfully listening or '0' fail to listen with provided port
 * @details Get the IP and Port from config handler
 *          Start listening with provided port number and IP
 */
int Client_Communication_Handler::startServer()
{
    //get server IP address
    serverIP = config->getSERVERdetail().ServerIP;
    qDebug()<<"serverIP="<<serverIP;

    //get server Port number
    serverPort = config->getSERVERdetail().ServerPort;

    //convert int to QString
    QString Dport = QString::number(serverPort);

    // Check if value is a valid integer (not float or non-numeric)
    bool isInt1 = false;
    Dport.toInt(&isInt1);
    if (!isInt1) {
        activityLog << "cannot start the dedicated tcp socket for client: dedicatedPort must be an integer value, actual: " << serverPort;
        return 0;
    }

    // check Detect clearly invalid types like alphabets-only strings
    if (Dport.contains(QRegularExpression("^[a-zA-Z\\s]+$"))) {
        activityLog << "cannot start the dedicated tcp socket for client: dedicatedPort appears to be a non-numeric string: " << serverPort;
        return 0;
    }

    if(serverPort<=0 || serverPort>65535 ){
        if(config->getLogLevel()>LOGLEVELINFO)
            activityLog<<"cannot start the dedicated tcp socket for client because port is Invalid";
        return 0;
    }

    // Regular expression for strict IPv4 format validation
    QRegularExpression ipv4Strict(R"(^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$)");

    if (serverIP.toString().isNull()|| !ipv4Strict.match(serverIP.toString()).hasMatch() || QHostAddress(serverIP).isNull()) {
        activityLog<<"In Data_Receiver_Thread received IP address is invalid";
        return 0;
    }

    // Try to start listening on provided network interface (IPv4/IPv6) at the given port
    if (!listen(serverIP, serverPort)) {
    //    if (!listen(QHostAddress::Any, serverPort)) {

        if(config->getLogLevel()>=LOGLEVELINFO){
            // If listening fails, print the error message to the debug output
            qDebug() << "Could not start dedicated server:" << errorString();
            activityLog<< "Could not start dedicated server:" << errorString();
        }

        // Close the server to clean up any partial setup
        close();

        // Return 0 indicating failure to start the server
        return 0;
    } else {
        // If listening is successful, print confirmation with the port number
        if(config->getLogLevel()>LOGLEVELINFO)
            activityLog<< "Server listening on port" << serverPort;
        qDebug()<<"Server listening on port" << serverPort;

        // Return 1 indicating the server started successfully
        return 1;
    }
}



/**
 * @brief   Function to send STATUS data to all connected clients
 * @param   STATUS structure
 * @return  None
 * @details Check the connected states of all connected clients
 *          If the client is connected, Take tha structure which is passing to this function
 *          Convert the strcuture into byte array
 *          And write it to each client
 *          If any client is disconnected, remove the connection with that client
 */
void Client_Communication_Handler::sendDataToAllClients(STATUS data)
{

    // Iterate over all connected clients stored in m_clients
    for (QTcpSocket* client : m_clients) {


        QString foundControllerID;

        // Search clients_details for matching tcpSocket pointer and take the controllerID to send
        for (auto it = clients_details.begin(); it != clients_details.end(); ++it) {
            if (it.value().tcpSocket == client) {
                foundControllerID = it.key();  // controllerID
                break;
            }
        }


        //check the controllerID is empty if it's empty return
        if (!foundControllerID.isEmpty()) {
            qDebug() << "Found controllerID:" << foundControllerID << "for socket:" << client;

        } else {
            qDebug() << "No controllerID found for socket:" << client;
            activityLog<<"No controllerID found for socket:" << client;
            return;
        }

        // Check if the client socket is connected and valid
        if (client->state() == QAbstractSocket::ConnectedState && client->isValid()) {

            // Prepare a buffer to store the serialized data
            QByteArray buffer;

            // Create a QDataStream to write data into the buffer
            QDataStream out(&buffer, QIODevice::WriteOnly);
            out.setByteOrder(QDataStream::LittleEndian);  // Ensure little-endian byte order

            if(config->getLogLevel()>LOGLEVELINFO){
                activityLog<<"data is sent to all clients";
                // Log the data values for debugging purposes
                activityLog << "controller_ID" << foundControllerID
                            << "readyForAcq" << data.readyForAcq
                            << "systemUp" << data.systemUp
                            << "startAcq" << data.startAcq
                            << "dedicatedIp" << data.dedicatedIp.toString()
                            << "dedicatedPort" << data.dedicatedPort;
            }

            // Serialize the STATUS structure fields into the stream
            out << foundControllerID
                << data.readyForAcq
                << data.systemUp
                << data.startAcq
                << data.dedicatedIp
                << data.dedicatedPort;

            // Write the serialized buffer to the client socket
            client->write(buffer);
            client->flush(); // Ensure data is sent immediately

            // Log the client's address and port to which data was sent
            if(config->getLogLevel()>LOGLEVELINFO)
                activityLog << "Sent to client Address :" << client->peerAddress().toString() << QString::number(client->peerPort());

        } else {
            // If the client is not connected or is invalid, log and remove it
            activityLog << "Client" << client->peerAddress().toString() << ":"
                        << QString::number(client->peerPort()) << "is not connected or invalid. Removing.";

            // Remove the disconnected or invalid client from the list
            m_clients.removeOne(client);

            // Schedule the socket for deletion
            client->deleteLater();
        }
    }

//    // Log that the function was called (this might not mean no clients were connected!)
//    activityLog<< "server wants to send data but no clients connected yet";
//    qDebug()<< "server wants to send data but no clients connected yet";
//    return;

}


/**
 * @brief   This function is used to accept the new client and Init the signal slot for it.
 * @param   None
 * @return  None
 * @details Accept the new connection
 *          Init the signal slot to read data from that client and make proper disconnect
 *          Store all client socket address into List, for furthure process
 */
void Client_Communication_Handler::newConnection()
{

    // Get the next incoming client connection from the pending queue
    QTcpSocket *clientSocket = nextPendingConnection();

    // If no client is available (unlikely, but safe), exit the function
    if (!clientSocket)
        return;

    // Connect the socket's 'disconnected' signal to a handler function
    // This ensures we handle cleanup when a client disconnects
    connect(clientSocket, &QTcpSocket::disconnected,
            this, &Client_Communication_Handler::clientDisconnected);

    // Connect the socket's 'readyRead' signal to a handler function
    // This allows reading incoming data from the client
    connect(clientSocket, &QTcpSocket::readyRead,
            this, &Client_Communication_Handler::readClientData);

    // Add the new client socket to the internal client list
    m_clients.append(clientSocket);

    // Log the connection details (IP and port) to the debug console
    if(config->getLogLevel()>LOGLEVELINFO)
        activityLog << "New client connected:" << clientSocket->peerAddress().toString() << ":" << clientSocket->peerPort();

}

/**
 * @brief   This function is used to disconnect the tcp socket
 * @param   None
 * @return  None
 * @details If any client is gets disconnected
 *          This function will do remove the socket connection for that client from the list of tcp sockets
 */
void Client_Communication_Handler::clientDisconnected()
{

    // Get the sender object (should be the QTcpSocket that emitted 'disconnected')
    QTcpSocket *clientSocket = qobject_cast<QTcpSocket*>(sender());

    // If it's valid, proceed with cleanup
    if (clientSocket) {
        // Log the disconnect event with the client's address and port
        qDebug() << "Client disconnected:"
                 << clientSocket->peerAddress().toString()
                 << ":" << clientSocket->peerPort();

        QString foundControllerID;
        // Iterate over all connected clients stored in m_clients
        for (QTcpSocket* client : m_clients) {




            // Search clients_details for matching tcpSocket pointer and take the controllerID to Log
            for (auto it = clients_details.begin(); it != clients_details.end(); ++it) {
                if (it.value().tcpSocket == client) {
                    foundControllerID = it.key();  // controllerID
                    break;
                }
            }
        }
        // Remove the client socket from the client list
        m_clients.removeOne(clientSocket);
        activityLog<<"client is disconnected "<<foundControllerID;

        // Schedule the socket object for deletion (safe way to destroy QObject-derived classes)
        clientSocket->deleteLater();
    }
}

/**
 * @brief   This function is used to handle the received data from clients
 * @param   None
 * @return  None
 * @details Check if byte is available in the socket or not
 *          If data is present in the sokcet it will read it
 *          Deserialize that byte array to STATUS structure
 *          Check the purpose of the data,weather the data is connection based or command based
 *          If the connection based it will create a new port number for that client
 *          And send the port number to the client and Initialize the client_handler to create dedicated connection for that client
 *          If that data is command based, ready for acquisition
 *          Just store into the List with the controllerID and wait for the next client data
 *          Once it's received command based data from all connected clients, It will send that message to master
 */
void Client_Communication_Handler::readClientData()
{
    // Get the client socket that triggered the signal (i.e., the sender)
    QTcpSocket *clientSocket = qobject_cast<QTcpSocket*>(sender());

    // Proceed only if the socket is valid and there's data available to read
    if (clientSocket && clientSocket->bytesAvailable() > 0) {
        // Read all available bytes from the client socket
        QByteArray data = clientSocket->readAll();

        // Create a QDataStream to deserialize the data
        QDataStream in(&data, QIODevice::ReadOnly);
        // Ensure that both ends (client and server) use the same byte order
        in.setByteOrder(QDataStream::LittleEndian);

        // Define a local STATUS structure to store the deserialized data
        STATUS status;
        // Deserialize the STATUS structure from the received data stream
        in >> status.controller_ID
                >> status.readyForAcq
                >> status.systemUp
                >> status.startAcq
                >> status.dedicatedIp
                >> status.dedicatedPort;

//        qDebug()<<"data received from client";
//        qDebug() << "received from client " << status.controller_ID << " " << status.readyForAcq
//                 << " " << status.systemUp << " " << status.startAcq << " "
//                 << status.dedicatedIp << " " << status.dedicatedPort;

        // Check if client is online but not ready to acquire
        if (status.systemUp == 1 && status.readyForAcq == 0) {
            if(clients_details.contains(status.controller_ID)){
                qDebug()<<"the controller is already connected";
            }else{
                // Prepare a response STATUS object with dedicated connection info
                STATUS send;
                // Assign a unique IP and available port to the client
                send.dedicatedIp = serverIP;
                send.dedicatedPort = getAvailablePort();
                send.controller_ID = status.controller_ID;
                send.systemUp = status.systemUp;
                send.startAcq = 0;
                send.readyForAcq = 0;

                // Store client-specific port and ID in the map
                client client_data;
                client_data.controllerID = status.controller_ID;
                client_data.dedicatedPort = send.dedicatedPort;
                client_data.tcpSocket = clientSocket;
                clients_details.insert(client_data.controllerID, client_data);

                // Create a dedicated client handler that listens on the assigned port
                Client_Handler *client_handler= new Client_Handler(serverIP, send.dedicatedPort);
                qDebug()<<"DedicatedIp::"<<serverIP.toString();

                //--------- Notify the client about the new assignment----------

                // Prepare a buffer to store the serialized data
                QByteArray buffer;

                // Create a QDataStream to write data into the buffer
                QDataStream out(&buffer, QIODevice::WriteOnly);
                out.setByteOrder(QDataStream::LittleEndian);  // Ensure little-endian byte order

                // Log the data values for debugging purposes
                qDebug() << "Data send to client : controller_ID" << send.controller_ID
                         << "readyForAcq" << send.readyForAcq
                         << "systemUp" << send.systemUp
                         << "startAcq" << send.startAcq
                         << "dedicatedIp" << send.dedicatedIp
                         << "dedicatedPort" << send.dedicatedPort;
                qDebug()<<"dedicated port send to client";

                // Serialize the STATUS structure fields into the stream
                out << send.controller_ID
                    << send.readyForAcq
                    << send.systemUp
                    << send.startAcq
                    << send.dedicatedIp
                    << send.dedicatedPort;

                // Write the serialized buffer to the client socket
                clientSocket->write(buffer);
                clientSocket->flush(); // Ensure data is sent immediately


            }
        }
        // If the client is already ready for acquisition
        else if (status.readyForAcq == 1) {

            //check the client is present in the map to store the ready for acq variables in it
            if (clients_details.contains(status.controller_ID)) {
                // Reference to actual struct
                client &cli = clients_details[status.controller_ID];
                //update the flag into the particular client structure in the map
                cli.readyForAcq = 1;
                if(config->getLogLevel()>LOGLEVELINFO){
                    activityLog<<status.controller_ID<<" controller is ready for acq";
                }
                for (const client &cli : clients_details) {
                    //check the readyForAcq flag is received from all client and set the bool as false if not received from all client
                    if (cli.readyForAcq != 1) {
                        allReady = false;
                        // Exit early if any client is not ready
                        break;
                    }else {
                        allReady = true;
                    }
                }
                //check the flag to emit the signal to master class slot
                if (allReady) {
                    // All clients are ready for acquisition
                    // then emit the signal for the next step here
                    qDebug()<<"singal emitted to start acq from CCH";
                    emit receiveDataFromSocket(1);
                } else {
                    // Some clients are still not ready
                    activityLog<<status.controller_ID <<"Waiting for all client to send ready for acq";
                }

            }else {
                //If the clientID is not present in the map but server getting readyforAcq flag from that client,
                //Print the error in log file
                activityLog<<"received readyforACQ flag from "<<status.controller_ID<<" but this controller is didn't get the dedicatedPort till yet";
            }



        }

    }

}

/**
              * @brief   UpdateSocketSendData is used to write the data to socket.
              * @param   Command
              * @return  None
              * @details Upon receiving signal from server_master_controller, this will get triggered
              *          And then take the structure and serialize as byte array then send data to all clients
              */
void Client_Communication_Handler::UpdateSocketSendData(const STATUS& Command)
{
    qDebug() << "data received from master to send to clientsocket----" << Command.systemUp;

    // Prepare the byte array and data stream
    QByteArray byteData;
    QDataStream out(&byteData, QIODevice::WriteOnly);
    out.setByteOrder(QDataStream::LittleEndian);

    // Serialize all fields of STATUS into the byte stream
    out << Command.controller_ID
        << Command.readyForAcq
        << Command.systemUp
        << Command.startAcq
        << Command.dedicatedIp
        << Command.dedicatedPort;

    // Send the serialized STATUS to all valid connected clients
    for (QTcpSocket *client : m_clients) {
        if (client && client->state() == QAbstractSocket::ConnectedState) {
            client->write(byteData);
            client->flush();
            qDebug() << "Sent to client:" << client->peerAddress();

            //Log the sent data
            activityLog << Command.controller_ID
                        << Command.readyForAcq
                        << Command.systemUp
                        << Command.startAcq
                        << Command.dedicatedIp.toString()
                        << Command.dedicatedPort;
        } else {
            qDebug() << "Skipping disconnected client.";
        }
    }
}



/**
 * @brief   Function to get the available port from system
 * @param   None
 * @return  Available Port
 * @details Check the available port of the system and return it.
 */
quint16 Client_Communication_Handler::getAvailablePort()
{
    QTcpServer tempServer;

    // Ask OS to assign any free port by passing 0
    tempServer.listen(QHostAddress::Any, 0);

    quint16 port = tempServer.serverPort();
    tempServer.close();
    return port;
}
