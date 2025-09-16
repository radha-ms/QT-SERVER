/**
  ***************************************************************************************************************************************
  * @file    Data_Receiver_Handler.cpp
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all information about the make a connection with DAQ client and receiving data.
  *
  * @details 1.Create a tcp socket with client's IP and port.
  *          2.Starts the Data_Receiver thread for continues listening.
  *          3.If any data comes put into the pre_segmentation queue.
  *          4.And wait for the next data.
  ******************************************************************************************************************************************/


/*
 *Header file which is included and used
 */
#include "Data_Receiver_Handler.h"

/**
 * @brief   Data_Receiver_Handler constructor is used to start the receiver thread
 * @param   DedicatedIp
 * @param   DedicatedPort
 * @param   pre_SegmentationQueue
 * @return  None
 * @details Get the passing parameters and store it in local variables as
 *          DedicatedIp, DedicatedPort, pre_SegmentationQueue
 *          Then start the execution
 */
Data_Receiver_Handler::Data_Receiver_Handler(QHostAddress IP, quint32 port, LockFreeQueue<QByteArray> *queue, QObject *parent)
    : QObject (parent), DedicatedIp(IP), DedicatedPort(port), pre_SegmentationQueue(queue){

    QtConcurrent::run(this, &Data_Receiver_Handler::dataGenerate1ms);
    QtConcurrent::run(this, &Data_Receiver_Handler::dataGenerate10ms);
    QtConcurrent::run(this, &Data_Receiver_Handler::dataGenerate50ms);
    QtConcurrent::run(this, &Data_Receiver_Handler::dataGenerate100ms);
    QtConcurrent::run(this, &Data_Receiver_Handler::dataGenerate1sec);


}


/**
 * @brief   Data_Receiver_Thread is used to receive a real-time data
 * @param   Ip
 * @param   port
 * @return  None
 * @details creates tcp socket with the Ip & Port
 *          Waits for the connection
 *          Reads the byteArray of data
 *          Then put into pre_segmentation queue.
 *          If the client is disconnected log the error and exit.
 */
/***************************************************IMPORTANT*********************************************************************/
//void Data_Receiver_Handler::Data_Receiver_Thread() {
//    QTcpServer server;


//    // Regular expression for strict IPv4 format validation
//    QRegularExpression ipv4Strict(R"(^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$)");

//    if (DedicatedIp.toString().isEmpty() || !ipv4Strict.match(DedicatedIp.toString()).hasMatch()) {
//        activityLog << "In client handler received IP address is invalid (bad format)";
//        exit(0);
//    }

//    // If passed format check, then try QHostAddress
//    QHostAddress addr(DedicatedIp.toString());
//    if (addr.isNull()) {
//        activityLog << "In client handler received IP address is null";
//        exit(0);
//    }


//    QString Dport = QString::number(DedicatedPort);
//    // Check if value is a valid integer (not float or non-numeric)
//    bool isInt1 = false;
//    Dport.toInt(&isInt1);
//    if (!isInt1) {
//        activityLog << "In Data_Receiver_Thread received tcp socket port for client: dedicatedPort must be an integer value, actual: " << DedicatedPort;
//        exit(0);
//    }

//    // check Detect clearly invalid types like alphabets-only strings
//    if (Dport.contains(QRegularExpression("^[a-zA-Z\\s]+$"))) {
//        activityLog << "In Data_Receiver_Thread received tcp socket port for client: dedicatedPort appears to be a non-numeric string: " << DedicatedPort;
//        exit(0);
//    }

//    if(DedicatedPort<=0 || DedicatedPort>65535 ){
//        activityLog<<"In Data_Receiver_Thread received tcp socket port for client because port is Invalid";
//        exit(0);
//    }

//    qDebug() << "Trying to listen on DedicatedIp:" << DedicatedIp << "and port:" << DedicatedPort;

//   // if (!server.listen(DedicatedIp, DedicatedPort)) {
//        if (!server.listen(QHostAddress::Any, DedicatedPort)) {

//        qWarning() << "Failed to listen on" << DedicatedIp.toString() << ":" << DedicatedPort;
//        return;
//    }
////listen(QHostAddress::Any, serverPort)

//    qDebug() << "Listening on" << DedicatedIp.toString() << ":" << DedicatedPort;
//    activityLog << "Listening on" << DedicatedIp.toString() << ":" << DedicatedPort;

//    if (!server.waitForNewConnection(-1)) {
//        qWarning() << "No incoming connection";
//        return;
//    }

//    QTcpSocket *clientSocket = server.nextPendingConnection();
//    if (!clientSocket) {
//        qWarning() << "Failed to get client connection";
//        return;
//    }

//    qDebug() << "Client connected from" << clientSocket->peerAddress().toString();
//    activityLog << "Client connected from" << clientSocket->peerAddress().toString();

//    while (clientSocket->state() == QAbstractSocket::ConnectedState) {
//        if (!clientSocket->waitForReadyRead(-1)) {
//            qWarning() << "Timeout or no data";
//            continue;
//        }

//        //Read all data from clientsocket and store in byteArray
//        QByteArray buffer = clientSocket->readAll();

//        //check the buffer size
//        if (!buffer.isEmpty()) {
//            qDebug()<<"data received form client "<<buffer.size();

//            //enqueue received byteArray into the pre-segmentation queue
//            pre_SegmentationQueue->enQueue(buffer);
//          //  qDebug()<<"iam at receiver Handler Nd presegQUEue is "<<pre_SegmentationQueue;
//        }
//    }

//    qDebug()<<"client is disconnected "<<clientSocket->peerPort() <<clientSocket->peerAddress().toString();
//    activityLog <<"client dedicated tcp seocket is disconnected "<<clientSocket->peerPort() <<clientSocket->peerAddress().toString();
//    clientSocket->close();
//    clientSocket->deleteLater();

//}

//Queue_Manager<QByteArray> *queue = new Queue_Manager<QByteArray>();
LockFreeQueue<QByteArray> *queue = new LockFreeQueue<QByteArray>();
// 1ms data generator thread
void Data_Receiver_Handler::dataGenerate1ms() {
    int PACKET_SIZE = 81;
    int counter = 0;
    QList<QByteArray> array;
    int amplitudeInput = 7;
    int InputHz = 2;
    int Sineupdatecount = 0;

    while (true) {
        QByteArray packet;
        qint64 currentUtcTime = QDateTime::currentDateTimeUtc().toMSecsSinceEpoch(); // Current UTC timestamp in ms

        // Create one ANALOG_DATA frame with sinusoidal data
        for (int id = 0; id < 1; id++) {
            ANALOG_DATA analog;
            analog.channelType = 'A';         // Mark as analog type
            analog.channelID = id;            // Channel ID
            analog.timestamp = currentUtcTime; // Timestamp
            analog.counter = counter;          // Incrementing counter for tracking

            // Simulate sine wave: A*sin(2πft), approximated for discrete update
            float Sineupdate = amplitudeInput * sin(3.1415 * InputHz * (Sineupdatecount + id) / 1024);
            analog.data = round(Sineupdate * 10000.0) / 10000.0; // Rounded to 4 decimal places

            memset(analog.padding, 9, sizeof(analog.padding)); // Fill padding with dummy values

            // Prepare binary packet from struct
            packet.resize(PACKET_SIZE);
            memcpy(packet.data(), &analog, PACKET_SIZE);

            array.append(packet);
        }

        Sineupdatecount++; // Move the sine wave phase

        // Combine all packets into one
        QByteArray combinedPacket;
        for (const QByteArray& packet : array)
            combinedPacket.append(packet);

        queue->enQueue(combinedPacket); // Push combined data into LockFreeQueue

        array.clear(); // Clear array for next loop
        counter++;     // Increment counter
        QThread::msleep(1); // Wait 1ms before generating next sample
//        qDebug()<<"1ms....";
    }
}

void Data_Receiver_Handler::dataGenerate10ms() {
    int PACKET_SIZE = 81;
    int counter = 0;
    QList<QByteArray> array;

    while (true) {
        QByteArray packet;
        qint64 currentUtcTime = QDateTime::currentDateTimeUtc().toMSecsSinceEpoch(); // Timestamp in ms

        // Generate data for channel 1
        for (int id = 1; id < 2; id++) {
            ANALOG_DATA analog;
            analog.channelType = 'A';
            analog.channelID = id;
            analog.timestamp = currentUtcTime;
            analog.counter = counter;
            analog.data = 0.1415f + counter; // Simple increasing float pattern

            memset(analog.padding, 9, sizeof(analog.padding)); // Padding

            packet.resize(PACKET_SIZE);
            memcpy(packet.data(), &analog, PACKET_SIZE);
            array.append(packet);
        }

        // Merge and send
        QByteArray combinedPacket;
        for (const QByteArray& packet : array)
            combinedPacket.append(packet);

        queue->enQueue(combinedPacket);
        array.clear();
        counter++;
        QThread::msleep(10); // Wait 100ms before next iteration
//        qDebug()<<"10ms....";

    }
}


void Data_Receiver_Handler::dataGenerate50ms() {
    int PACKET_SIZE = 81;
    int counter = 0;
    QList<QByteArray> array;

    while (true) {
        QByteArray packet;
        qint64 currentUtcTime = QDateTime::currentDateTimeUtc().toMSecsSinceEpoch(); // Timestamp in ms

        // Generate data for channel 1
        for (int id = 2; id < 3; id++) {
            ANALOG_DATA analog;
            analog.channelType = 'A';
            analog.channelID = id;
            analog.timestamp = currentUtcTime;
            analog.counter = counter;
            analog.data = 0.1415f + counter; // Simple increasing float pattern

            memset(analog.padding, 9, sizeof(analog.padding)); // Padding

            packet.resize(PACKET_SIZE);
            memcpy(packet.data(), &analog, PACKET_SIZE);
            array.append(packet);
        }

        // Merge and send
        QByteArray combinedPacket;
        for (const QByteArray& packet : array)
            combinedPacket.append(packet);

        queue->enQueue(combinedPacket);
        array.clear();
        counter++;
        QThread::msleep(50); // Wait 100ms before next iteration
   //     qDebug()<<"50ms....";

    }
}


// 100ms data generator thread
void Data_Receiver_Handler::dataGenerate100ms() {
    int PACKET_SIZE = 81;
    int counter = 0;
    QList<QByteArray> array;

    while (true) {
        QByteArray packet;
        qint64 currentUtcTime = QDateTime::currentDateTimeUtc().toMSecsSinceEpoch(); // Timestamp in ms

        // Generate data for channel 1
        for (int id = 3; id < 4; id++) {
            ANALOG_DATA analog;
            analog.channelType = 'A';
            analog.channelID = id;
            analog.timestamp = currentUtcTime;
            analog.counter = counter;
            analog.data = 0.1415f + counter; // Simple increasing float pattern

            memset(analog.padding, 9, sizeof(analog.padding)); // Padding

            packet.resize(PACKET_SIZE);
            memcpy(packet.data(), &analog, PACKET_SIZE);
            array.append(packet);
        }

        // Merge and send
        QByteArray combinedPacket;
        for (const QByteArray& packet : array)
            combinedPacket.append(packet);

        queue->enQueue(combinedPacket);
        array.clear();
        counter++;
        QThread::msleep(100); // Wait 100ms before next iteration
      //  qDebug()<<"100ms....";

    }
}

// 1 second data generator thread
void Data_Receiver_Handler::dataGenerate1sec() {
    int PACKET_SIZE = 81;
    int counter = 0;
    QList<QByteArray> array;

    while (true) {
        QByteArray packet;
        qint64 currentUtcTime = QDateTime::currentDateTimeUtc().toMSecsSinceEpoch(); // Get current timestamp

        // Generate data for channel 2
        for (int id = 4; id < 5; id++) {
            ANALOG_DATA analog;
            analog.channelType = 'A';
            analog.channelID = id;
            analog.timestamp = currentUtcTime;
            analog.counter = counter;
            analog.data = 0.1415f + counter;

            memset(analog.padding, 9, sizeof(analog.padding)); // Padding

            packet.resize(PACKET_SIZE);
            memcpy(packet.data(), &analog, PACKET_SIZE);
            array.append(packet);
        }

        // Combine all packets into one
        QByteArray combinedPacket;
        for (const QByteArray& packet : array)
            combinedPacket.append(packet);

        queue->enQueue(combinedPacket); // ✅ This was fixed from mistakenly calling deQueue before
        array.clear();
        counter++;
        QThread::msleep(200); // Wait for 1 second before next data
       // qDebug()<<"1000ms....";

    }
}

// This thread keeps running and moves data from internal queue to preSegmentation queue
void Data_Receiver_Handler::Data_Receiver_Thread() {
    qDebug() << "data receiver thread started";

    while (true) {
        QByteArray buffer;

        // Try to pop data from the queue
        if (queue->deQueue(buffer)) {
            // If successful, forward it to next stage (preSegmentation queue)
            pre_SegmentationQueue->enQueue(buffer);
            // qDebug() << "Buffer dequeued and passed to pre_segmentation";
        } else {
            // Sleep briefly to avoid tight loop if queue is empty
            QThread::msleep(1);
        }
    }
}
