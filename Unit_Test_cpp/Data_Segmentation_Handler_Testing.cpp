


#include <QtTest/QtTest>
#include "Data_Segmentation_Handler.h"


class Data_Segmentation_Handler_Test : public QObject{
    Q_OBJECT

private slots:
    //checking that thread is getting called or not
    void testing();
};



/**
 * @brief   Data_Segmentation_Handler_Test::testing
 * @param   None
 * @return  None
 * @details This function is used to check data segmentation thread is starts running or not
 */
void Data_Segmentation_Handler_Test::testing(){
    Data_Segmentation_Handler *segment;

    segment->Data_Segmentation_Thread();
}


//QTEST_MAIN(Data_Segmentation_Handler_Test)
#include "Data_Segmentation_Handler_Testing.moc"
