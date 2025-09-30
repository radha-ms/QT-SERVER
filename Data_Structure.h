/**
 * @file    Data_Structure.h
 * @author  karthikeya.sr
 * @version V1.0
 * @date    02-May-2025
 *
 * @brief   Defines various data structures used for
 *          test parameters, and real-time data acquisition.
 *
 * @details This header contains key data structures for:
 *          - Representing analog and digital data channels
 *          - Encapsulating raw real-time data from the controller
 *          - Supporting the internal operation of the acquisition and processing modules
 *
 * @note    use Singleton pattern combined with thread-safe access via QMutex.
 */

#ifndef DATA_STRUCTURE_H
#define DATA_STRUCTURE_H

#include <QObject>
#include <QMap>
#include <QVector>
#include <qhostaddress.h>
#include <QTcpServer>
#include <variant>



/**
 * @brief   Structure representing server details.
 *
 * @details This structure is used to store all parameter related to the DAQ server
 */
typedef struct{
    QHostAddress ServerIP;
    quint32 ServerPort;
}SERVER;

/**
 * @brief   Structure representing test details.
 *
 * @details This structure is used to store all parameter related to the DAQ test configuration
 */
typedef struct{
    int Refresh_Rate;
    QMap<int, int> ChannelID_SamplingRate;
}TEST;


/**
 * @brief   Structure representing kafka details.
 *
 * @details This structure is used to store all parameter related to the DAQ kafka configuration
 */
typedef struct{
    QString kafkaURL;
    QString serializer;
    int NoOfAnalogTopic;
    int NoOfDigitalTopic;
}KAFKA;

/**
 * @brief   Structure representing kafka details.
 *
 * @details This structure is used to store all parameter related to the DAQ kafka configuration
 */
typedef struct{
    QString topic_name;
    QString serializer;
    QString deSerializer;
}COMMUNICATION_TOPIC;


/**
 * @brief   Structure representing status Command.
 *
 * @details This structure is used to share the success or failure Command from each modules to server_master_controller
 */
typedef struct{
    int status;
    QString Command;
    int startACQ;
}KAFKA_STATUS;
//Q_DECLARE_METATYPE(KAFKA_STATUS)

/**
 * @brief   Structure representing status Command.
 *
 * @details This structure is used to share the commands or Initial communication details between DAQ server and DAQ client
 */
typedef struct{
    QString controller_ID;
    quint8 readyForAcq;
    quint8 systemUp;
    quint8 startAcq;
    QHostAddress dedicatedIp;
    quint32 dedicatedPort;
}STATUS;
Q_DECLARE_METATYPE(STATUS)

/**
  * @brief   Structure representing status Command.
  *
  * @details This structure is used to store the Information of the each client.
  */
typedef struct{

    QString clientID;
    QString dedicatedIP;
    quint32 DedicatedPort;
    QTcpSocket* socket;
    quint16 dataPort;
    QTcpServer* dataServer;
}CLIENT_INFO;


/**
 * @brief   Structure representing analog topic details.
 *
 * @details This structure is used to store a single analog topic name with Bandwidth range
 */
typedef struct{
    QString topicName;
    int minimum_sampling_rate;
    int maximum_sampling_rate;
}Analog_output_topic;

/**
 * @brief   Structure representing digital topic details.
 *
 * @details This structure is used to store a single analog topic name with Bandwidth range
 */
typedef struct{
    QString topicName;
    int sampling_rate;
}Digital_output_topic;


/**
 * @brief   Structure representing analog data from a channel.
 *
 * @details This structure is used to encapsulate a single sample of analog data received
 *          from a client socket.
 */
#pragma pack(push, 1)
typedef struct{
    char  channelType;
    int   channelID ;
    qint64  timestamp;
    int   counter;
    float data;
    quint8 padding[60];
}ANALOG_DATA; //81 bytes
#pragma pack(pop)


/**
 * @brief   Structure representing analog data from a channel.
 *
 * @details This structure is used to encapsulate a 16 sample of digital data received
 *          from a specific channel's in the data acquisition system.
 */
#pragma pack(push, 1)
typedef struct{
    char channelType;
    int cardID;
    qint64 timestamp;
    int counter;
    bool data[64];
}DIGITAL_DATA; //81 bytes
#pragma pack(pop)

//decleration for used in data receiver module and segment module
typedef std::variant<ANALOG_DATA, DIGITAL_DATA> DataPacket;




#endif // DATA_STRUCTURE_H
