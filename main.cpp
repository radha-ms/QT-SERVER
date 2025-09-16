/**
  ***********************************************************************************************************************************
  * @file    main.cpp
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   Entry point of the application that initializes the Qt core application
  *          and starts the Server_Master_Controller to handle configuration and Initialize threads.
  ***********************************************************************************************************************************
*/



/* Header file which include and used */
#include <QCoreApplication>
#include <Server_Master_Controller.h>

/**
 * @brief main method
 * @param argc
 * @param argv
 * @return QCoreApplication
 * @details create a object for server master class to initialize all files
 */
int main(int argc, char *argv[])
{
    QCoreApplication a(argc, argv);


    //Create temp lock file to avoid multiple instance
    QLockFile lockFile(QDir::temp().absoluteFilePath("DACS_Server_v1.0.lock"));
    /* Trying to close the Lock File, if the attempt is unsuccessful for 100 milliseconds,
         * then there is a Lock File already created by another process.
         / Therefore, we throw a warning and close the program
         * */
    if(!lockFile.tryLock(100))
    {
        qDebug()<<"DACS_Server Application instance is already running.\n";
        qDebug()<<"User is allowed to run only one instance of DACS Application. Close the open application and run again";
        return 1;
    }


    //Initialize server master controller class here
    Server_Master_Controller *master = new Server_Master_Controller();
    return a.exec();
}
