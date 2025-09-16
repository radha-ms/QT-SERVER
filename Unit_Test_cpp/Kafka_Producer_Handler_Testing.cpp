





#include <QtTest/QtTest>
#include "Kafka_Producer_Handler.h"


class Kafka_Producer_Handler_Test : public QObject{
    Q_OBJECT

private slots:
    //checking that thread is getting called or not
    void testing();
};



/**
 * @brief   Kafka_Producer_Handler_Test::testing
 * @param   None
 * @return  None
 * @details This function is used to check data segmentation thread is starts running or not
 */
void Kafka_Producer_Handler_Test::testing(){
    Kafka_Producer_Handler *produce;


    produce->kafka_Producer_Thread();
}


//QTEST_MAIN(Kafka_Producer_Handler_Test)
#include "Kafka_Producer_Handler_Testing.moc"
