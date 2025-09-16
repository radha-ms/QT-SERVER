


#include <QtTest/QtTest>
#include "Server_Master_Controller.h"


class Server_Master_Controller_Test : public QObject{
    Q_OBJECT

private slots:

    //check that onConfigLoaded function is Inited all modules or not
    void testing();
    //check that HandleKafkaReceivedData function is emitting signal to tcp socket
    void HandleKafkaReceivedData();
    //check that UpdateSocketReceivedData function is emitting signal to kafka
    void UpdateSocketReceivedData();
};

/**
 * @brief Server_Master_Controller_Test::testing
 * @param   None
 * @return  None
 * @details This function is used to check the onconfigLoad function is Initing all modules
 */
void Server_Master_Controller_Test::testing(){
    qDebug()<<"entered testing";
    Server_Master_Controller *master;
    QString data = "";
    int flag=0;
    master->onConfigLoaded( data,  flag);
}

/**
 * @brief Server_Master_Controller_Test::HandleKafkaReceivedData
 * @param   None
 * @return  None
 * @details This function is used to check the HandleKafkaReceivedData is emitting signal
 */
void Server_Master_Controller_Test::HandleKafkaReceivedData(){
  Server_Master_Controller *master;
  QJsonObject data;
  data["key"] = 1000;
  data["acquisitionFlag"] = 1;
  master->HandleKafkaReceivedData(data);
}

/**
 * @brief Server_Master_Controller_Test::UpdateSocketReceivedData
 * @param   None
 * @return  None
 * @details This function is used to check the UpdateSocketReceivedData is emitting signal
 */
void Server_Master_Controller_Test::UpdateSocketReceivedData(){
    Server_Master_Controller *master;
    master->UpdateSocketReceivedData(1);
}
//QTEST_MAIN(Server_Master_Controller_Test)
#include "Server_Master_Controller_Testing.moc"
