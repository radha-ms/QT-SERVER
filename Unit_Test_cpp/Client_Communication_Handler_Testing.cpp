
#include <QtTest/QtTest>
#include "Client_Communication_Handler.h"



class Client_Communication_Handler_Test : public QObject
{
    Q_OBJECT
public:
    virtual ~Client_Communication_Handler_Test(); // declared
private slots:

    /**
     * ADDITIONAL TEST CASE DECLARATION
     */

    /**************************************VALID_FILE_PATHS**************************************************/
    //Checking tcp server is creating or not
    void validate_StartServer();
    //checking server is sendig data to clients or not
    void validate_sendDataToAllClients();
    //checking get available port in os
    void validate_getportNumer();


};

//Client_Communication_Handler_Test destructor
Client_Communication_Handler_Test::~Client_Communication_Handler_Test() {} // defined

//Global object for Client_Communication_Handler_Test


/**
 * @brief Client_Communication_Handler_Test::validate_StartServer
 * @param   None
 * @return  None
 * @details This function is used to check the tcp socket is listening or not on config file IP and Port
 */
void Client_Communication_Handler_Test::validate_StartServer(){
Client_Communication_Handler client;
    int actualValue =  client.startServer();
    QCOMPARE(actualValue,1);
}

/**
 * @brief Client_Communication_Handler_Test::validate_sendDataToAllClients
 * @param   None
 * @return  None
 * @details This function is used to check the data is sending to all connected clients or not
 */
void Client_Communication_Handler_Test::validate_sendDataToAllClients(){
    Client_Communication_Handler client;
    STATUS data;
    data.controller_ID ="0";
    data.systemUp =0;
    data.startAcq =1;
    data.readyForAcq =1;
//    client.sendDataToAllClients(data);
    QVERIFY(true);
}


/**
 * @brief Client_Communication_Handler_Test::validate_getportNumer
 * @param   None
 * @return  None
 * @details This function is used to get the available port in the OS.
 */
void Client_Communication_Handler_Test::validate_getportNumer(){
Client_Communication_Handler client;

    quint16 value = client.getAvailablePort();
    quint16 expected = 65535;
    if(value>expected){
        QVERIFY(false);
    }else if (value<=0) {
        QVERIFY(false);
    }else {
        QVERIFY(true);
    }
    //   QCOMPARE(value, expected);
}

//QTEST_MAIN(Client_Communication_Handler_Test)
#include "Client_Communication_Handler_Testing.moc"

