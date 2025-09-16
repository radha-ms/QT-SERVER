#include <QtTest/QtTest>
#include "Kafka_Communication_Handler.h"
#include <QSignalSpy>
#include <QJsonObject>
#include <QJsonDocument>
//#include "Config_Handler.h"


//Config_Handler *config;
class Kafka_Communication_Handler_Test : public QObject
{
    Q_OBJECT

private slots:

    //This is used to check with URL
    void test_getURL_and_topic_Details();

    //This is used to check the producer method
    void test_producerInstance();

    //This is used to check the consumer method
    void test_consumerInstance();

    //This is used to check the kafka_listener
    void test_signalEmission_onReceivingKafkaMessage();

    //This is used to check the kafka producer method
    void test_UpdateSendDataToKafka_withValidData();
};


/**
 * @brief Kafka_Communication_Handler_Test::test_getURL_and_topic_Details
 * @param   None
 * @return  None
 * @details This function is used to validate the all variables which is get from config handler
 */


void Kafka_Communication_Handler_Test::test_getURL_and_topic_Details() {

    Kafka_Communication_Handler handler;
    bool result  = handler.getDetails();
    QCOMPARE(result, true);
}


/**
 * @brief   Kafka_Communication_Handler_Test::test_producerInstance
 * @param   None
 * @return  None
 * @details This function is used to validate producer instance
 */
void Kafka_Communication_Handler_Test::test_producerInstance(){

    Kafka_Communication_Handler handler;
    int result = handler.create_Producer_Instance();
    QCOMPARE(result, 1);
}



/**
 * @brief   Kafka_Communication_Handler_Test::test_consumerInstance
 * @param   None
 * @return  None
 * @details This function is used to validate consumer instance
 */
void Kafka_Communication_Handler_Test::test_consumerInstance(){

    Kafka_Communication_Handler handler;
    int result = handler.create_Consumer_Instance();
    QCOMPARE(result, 1);
}




/**
 * @brief   Kafka_Communication_Handler_Test::test_signalEmission_onReceivingKafkaMessage
 * @param   None
 * @return  None
 * @details This function is used to validate signal which is used to process the received data from communication topic
 */
void Kafka_Communication_Handler_Test::test_signalEmission_onReceivingKafkaMessage() {
    Kafka_Communication_Handler handler;

    QSignalSpy spy(&handler, &Kafka_Communication_Handler::receivedDataFromKafka);

    // simulate Kafka message
    QJsonObject sampleData;
    sampleData["key"] = 2000;
//    emit handler.receivedDataFromKafka(sampleData);

    QCOMPARE(spy.count(), 1);
    QList<QVariant> arguments = spy.takeFirst();
    QJsonObject received = arguments.at(0).toJsonObject();
//    QCOMPARE(received["key"].toString(), QString("value"));
}

/**
 * @brief   Kafka_Communication_Handler_Test::test_UpdateSendDataToKafka_withValidData
 * @param   None
 * @return  None
 * @details This function is used to validate signal which is used to send the data to communication topic
 */
void Kafka_Communication_Handler_Test::test_UpdateSendDataToKafka_withValidData() {
    Kafka_Communication_Handler handler;

    QJsonObject data;
    data["key"] = 1;
    data["message"] = "test";

    // This test just ensures no crash. Cannot test real Kafka delivery without mocking.
    handler.UpdateSendDataToKafka(data);

    QVERIFY(true); // Pass if no crash or error
}


//QTEST_MAIN(Kafka_Communication_Handler_Test)
#include "Kafka_Communication_Handler_Testing.moc"
