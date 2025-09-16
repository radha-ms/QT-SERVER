///**
//  ***************************************************************************************************************************************
//  * @file    Application_Activity_Logger.h
//  * @author  karthikeya.sr
//  * @version V1.0
//  * @date    02-May-2025
//  * @brief   This file provides all headers to Application_Activity_Logger.cpp
//  ********************************************************************************************************************************************/

//#ifndef Application_Activity_Logger_H
//#define Application_Activity_Logger_H

//#include <QObject>
//#include <QTimer>
//#include <QDir>
//#include <QDateTime>
//#include <QDebug>
//#include "QSettings"
//#include "qtextstream.h"
//#include <qmutex.h>



//class Application_Activity_Logger
//{
//public:
//    Application_Activity_Logger() = delete; // No object creation
//    static void printf(const QString &Data);

//private:
//    static bool ACTIVITY_FileCreation;
//    static QFile *ACTIVITY_LOG;
//};

//#endif // Application_Activity_Logger_H



/**
  ***************************************************************************************************************************************
  * @file    Application_Activity_Logger.h
  * @author  karthikeya.sr
  * @version V1.0
  * @date    02-May-2025
  * @brief   This file provides all headers to Application_Activity_Logger.cpp
  ********************************************************************************************************************************************/

#ifndef Application_Activity_Logger_H
#define Application_Activity_Logger_H

#include <QObject>
#include <QTimer>
#include <QDir>
#include <QDateTime>
#include <QDebug>
#include "QSettings"
#include "qtextstream.h"
#include <qmutex.h>



class Application_Activity_Logger
{
public:
    Application_Activity_Logger();
    ~Application_Activity_Logger();

        template <typename T>
        Application_Activity_Logger& operator<<(const T& value) {
            stream << value;
            return *this;
        }

private:
        static QFile* logFile;
           static bool isInitialized;
           static void initializeLogFile();
           QString buffer;
           QTextStream stream;
};

#define activityLog Application_Activity_Logger()

#endif // Application_Activity_Logger_H

