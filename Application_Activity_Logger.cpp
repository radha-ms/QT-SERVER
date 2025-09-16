///**
//  ***************************************************************************************************************************************
//  * @file    Application_Activity_Logger.cpp
//  * @author  karthikeya.sr
//  * @version V1.0
//  * @date    02-May-2025
//  * @brief   This file provides all information about the Logging the debug statements in to the log file.
//  *
//  * @details 1.Get the directory to create a issue log file
//  *          2.Create a log file with the naming of  current-Data&Time-ActivityLog.txt
//  *          3.Write the data into the log file.
//  *
//  ******************************************************************************************************************************************/


//#include "Application_Activity_Logger.h"



//// Initialize static variables
//bool Application_Activity_Logger::ACTIVITY_FileCreation = false;
//QFile* Application_Activity_Logger::ACTIVITY_LOG = nullptr;


///**
// * @brief This fucntion is used to create the log file and log the data into the file.
// * @param Data which is want to write into the log file.
// * @return None
// * @details Get the filepath where it's need to create
// *          Create a file with proper naming
// *          Open that file for writing
// *          Write the data into the log file.
// */
//void activityLog<<const QString &Data)
//{
//    if(!ACTIVITY_FileCreation)
//    {
//        ACTIVITY_FileCreation =true;
//        QString outputDir;

//        outputDir= QDir::currentPath() + "/issue_log";

//        QString path=outputDir;

//        QDir dir;
//        if(!dir.exists(path))
//        {
//            dir.mkpath(path);
//        }

//        ACTIVITY_LOG = new QFile();
//        QString filename = "/"+QDateTime::currentDateTime().toString("yyyy-MM-dd-hh-mm-ss")+"-ActivityLog.txt";
//        QString IssueFilename = outputDir+filename;
//        ACTIVITY_LOG = new QFile(IssueFilename);
//        if(!ACTIVITY_LOG->open(QIODevice::WriteOnly | QIODevice::Append))
//            qDebug()<<"Issue Log File Not Opened";
//    }

//    if(ACTIVITY_LOG->isOpen())
//    {
//        QTextStream  IssueStream(ACTIVITY_LOG);
//        IssueStream<<Data<<"\n";
//        IssueStream<<"\n";
//    }
//}



/**
  ***************************************************************************************************************************************
  * @file    Application_Activity_Logger.cpp
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   Implementation file for activity logging functionality.
  *
  * @details This file contains the implementation of the Application_Activity_Logger class, which is responsible for
  *          creating and managing a log file for recording runtime activity or debug information.
  *
  *          Key functionalities include:
  *            - Creating a dedicated "issue_log" directory in the current working path if it doesn't exist.
  *            - Generating a uniquely named log file using the current date and time format (yyyy-MM-dd-hh-mm-ss).
  *            - Writing messages or debug data to the log file using stream-style syntax (similar to qDebug()).
  *            - Ensuring the log file is created only once per application session.
  *
  *          The logger writes all collected log messages to the file when the logger object is destroyed.
  *
  ***************************************************************************************************************************************
  */



#include "Application_Activity_Logger.h"

///> Initialize static variables
QFile* Application_Activity_Logger::logFile = nullptr;
bool Application_Activity_Logger::isInitialized = false;


/**
 * @brief Constructor for Application_Activity_Logger.
 *
 * @details Initializes the internal text stream buffer and ensures the log file is created only once per session.
 *          If the log file is not yet initialized, it calls initializeLogFile() to set it up.
 */

Application_Activity_Logger::Application_Activity_Logger()
: stream(&buffer)
{
    if (!isInitialized) {
          initializeLogFile();
    }
}

/**
 * @brief Destructor for Application_Activity_Logger.
 *
 * @details When the logger object goes out of scope, it writes the accumulated buffer content to the log file
 *          along with a newline, and flushes the stream to ensure data is saved to disk.
 */
Application_Activity_Logger::~Application_Activity_Logger()
{
    if (logFile && logFile->isOpen()) {
            QTextStream fileStream(logFile);
            fileStream << buffer << "\n";
            fileStream.flush();
        }
}

/**
 * @brief Initializes the activity log file.
 *
 * @details Creates a directory named "issue_log" in the current working path if it doesn't already exist.
 *          Then creates a new log file with a name formatted as "yyyy-MM-dd-hh-mm-ss-ActivityLog.txt".
 *          Opens the file in append mode for writing log entries.
 *
 * @note This method is only called once per application session. Subsequent loggers reuse the same file.
 */
void Application_Activity_Logger::initializeLogFile() {
    isInitialized = true;

    QString outputDir = QDir::currentPath() + "/issue_log";
    QDir dir;
    if (!dir.exists(outputDir)) {
        dir.mkpath(outputDir);
    }

    QString filename = "/" + QDateTime::currentDateTime().toString("yyyy-MM-dd-hh-mm-ss") + "-ActivityLog.txt";
    QString fullPath = outputDir + filename;

    logFile = new QFile(fullPath);
    if (!logFile->open(QIODevice::WriteOnly | QIODevice::Append | QIODevice::Text)) {
        qWarning() << "Issue Log File Not Opened";
    }
}
