

#include <QtTest/QtTest>
#include "Client_Handler.h"


class Client_Handler_test : public QObject
{
    Q_OBJECT
private slots:

    /**
     * ADDITIONAL TEST CASE DECLARATION
     */

    /**************************************VALID_FILE_PATHS**************************************************/
    //checking that IP add is empty
    void IP_Is_Empty();
    //Checking that IP add is Invalid_special_IP address
    void Invalid_char_In_IP();
    //checking invalid format IP address
    void Invalid_Format_IP();
    //checking port value is empty
    void Port_Is_Empty();
    //checking that port is not integer
    void Port_IsFloat();
    //checking with valid input to get success
    void valid_Inputs();


};
//global pointer of client handler to access


/**
 * @brief   Client_Handler_test::IP_Is_Empty
 * @param   None
 * @return  None
 * @details This function is used to check the IP Address is empty
 */
void Client_Handler_test::IP_Is_Empty(){
Client_Handler *client;

    //should return 0
    int value  = client->spawn_Threads(QHostAddress(" "),55669);
    QCOMPARE(value,0);
}


/**
 * @brief   Client_Handler_test::Invalid_char_In_IP
 * @param   None
 * @return  None
 * @details This function is used to check the IP is contains special chars or not
 */
void Client_Handler_test::Invalid_char_In_IP(){
Client_Handler *client;

    //should return 0
    int value  = client->spawn_Threads(QHostAddress("192.168.2.@"),55669);
    QCOMPARE(value,0);

}


/**
 * @brief   Client_Handler_test::Invalid_Format_IP
 * @param   None
 * @return  None
 * @details This function is used to check the IP is invalid format
 */
void Client_Handler_test::Invalid_Format_IP(){
Client_Handler *client;

    //should return 0
    int value  = client->spawn_Threads(QHostAddress("abc.def.ghi.jkl"),55669);
    QCOMPARE(value,0);
}


/**
 * @brief   Client_Handler_test::Port_Is_Empty
 * @param   None
 * @return  None
 * @details This function is used to check the Port is empty or not
 */
void Client_Handler_test::Port_Is_Empty() {
Client_Handler *client;

    //should return 0
    int value  = client->spawn_Threads(QHostAddress("192.168.2.108"), 0);  // This is okay syntactically, but 0 is invalid for a port.
    QCOMPARE(value, 0);
}


/**
 * @brief   Client_Handler_test::Port_IsFloat
 * @param   None
 * @return  None
 * @details This function is used to check the Port is Interger or not
 */
void Client_Handler_test::Port_IsFloat() {
  Client_Handler *client;

    int fakeFloat = static_cast<int>(-132.134);  // Be explicit: C++ does not accept float here

    //should return 0
    int value = client->spawn_Threads(QHostAddress("192.168.2.108"), fakeFloat);
    QCOMPARE(value, 0);
}

/**
 * @brief   Client_Handler_test::valid_Inputs
 * @param   None
 * @return  None
 * @details This function is used to check the IP and Port is valid or not
 *          Spawing threads Data receiver thread and Data segmentation thread
 */
void Client_Handler_test::valid_Inputs(){

    Client_Handler *client;

    //should return 1
    int value  = client->spawn_Threads(QHostAddress("192.168.2.108"), 55669);
    QCOMPARE(value,1);
}

//QTEST_MAIN(Client_Handler_test)
#include "Client_Handler_Testing.moc"
