


#include <QtTest/QTest>
#include "Data_Receiver_Handler.h"
#include "LockFreeQueue.h"
#include <QByteArray>


class Data_Receiver_Handler_test : public QObject{
    Q_OBJECT

private slots:
    //Checking that the thread is creating or not
    void testing();
};


/**
 * @brief   Data_Receiver_Handler_test::testing
 * @param   None
 * @return  None
 * @details This function is used to check the data receiver thread is successfully creating or not
 */
//void Data_Receiver_Handler_test::testing(){

//    quint32 port = 55661;
//    LockFreeQueue()<QByteArray> *queue = new LockFreeQueue<QByteArray>();


//    // data receiver Thread Setup
//    QThread *receiver_Thread = new QThread;
//    //initialize the data receiver handler and passing IP and Port and rawdata queue.
//    Data_Receiver_Handler *worker = new Data_Receiver_Handler(QHostAddress("192.168.2.108"),  port, queue);
//    worker->moveToThread(receiver_Thread);

//    //initialize signal slot assinging the thread function to start and quit and delete
//    connect(receiver_Thread, &QThread::started, worker, &Data_Receiver_Handler::Data_Receiver_Thread);
//    connect(worker, &Data_Receiver_Handler::finished, receiver_Thread, &QThread::quit);
//    connect(worker, &Data_Receiver_Handler::finished, worker, &QObject::deleteLater);
//    connect(receiver_Thread, &QThread::finished, receiver_Thread, &QObject::deleteLater);
//    //start the thread
//    receiver_Thread->start();

//}

void Data_Receiver_Handler_test::testing()
{
    quint32 port = 55661;
    LockFreeQueue<QByteArray>* queue = new LockFreeQueue<QByteArray>();

    // Data receiver Thread Setup
    QThread* receiver_Thread = new QThread;

    // Initialize the data receiver handler and pass IP, port, and raw data queue
    Data_Receiver_Handler* worker = new Data_Receiver_Handler(QHostAddress("192.168.2.108"), port, queue);
    worker->moveToThread(receiver_Thread);

    // Connect signals and slots
    connect(receiver_Thread, &QThread::started, worker, &Data_Receiver_Handler::Data_Receiver_Thread);
    connect(worker, &Data_Receiver_Handler::finished, receiver_Thread, &QThread::quit);
    connect(worker, &Data_Receiver_Handler::finished, worker, &QObject::deleteLater);
    connect(receiver_Thread, &QThread::finished, receiver_Thread, &QObject::deleteLater);

    receiver_Thread->start();

    // Optional: Wait a bit and check status
    QTest::qWait(1000);  // Wait 1 second to allow thread to start
}


//QTEST_MAIN(Data_Receiver_Handler_test)
#include "Data_Receiver_Handler_Testing.moc"
