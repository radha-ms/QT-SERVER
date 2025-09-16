QT += core
QT -= gui
QT       += sql
CONFIG   += qt
QT       += network
QT       += concurrent
QT       += testlib

CONFIG += c++17 console
CONFIG -= app_bundle

# The following define makes your compiler emit warnings if you use
# any Qt feature that has been marked deprecated (the exact warnings
# depend on your compiler). Please consult the documentation of the
# deprecated API in order to know how to port your code away from it.
#DEFINES += UNIT_TEST
DEFINES += QT_DEPRECATED_WARNINGS

# You can also make your code fail to compile if it uses deprecated APIs.
# In order to do so, uncomment the following line.
# You can also select to disable deprecated APIs only up to a certain version of Qt.
#DEFINES += QT_DISABLE_DEPRECATED_BEFORE=0x060000    # disables all the APIs deprecated before Qt 6.0.0


contains(DEFINES, UNIT_TEST) {

SOURCES += \
#    Unit_Test_cpp/Config_Handler_Testing.cpp \
#    Unit_Test_cpp/Kafka_Communication_handler_Testing.cpp \
#    Unit_Test_cpp/Client_Communication_Handler_Testing.cpp \
#    Unit_Test_cpp/Client_Handler_Testing.cpp \
#    Unit_Test_cpp/Data_Receiver_Handler_Testing.cpp \
#    Unit_Test_cpp/Data_Segmentation_Handler_Testing.cpp \
#     Unit_Test_cpp/Kafka_Producer_Handler_Testing.cpp \
#    Unit_Test_cpp/Server_Master_Controller_Testing.cpp \
    Application_Activity_Logger.cpp \
    Server_Master_Controller.cpp \
    Config_Handler.cpp \
    Kafka_Communication_Handler.cpp \
    Client_Communication_Handler.cpp \
    Kafka_Producer_Handler.cpp \
    Client_Handler.cpp \
    Data_Receiver_Handler.cpp \
    Data_Segmentation_Handler.cpp \
#    Temp_Consumer.cpp

HEADERS += \
    Config_Handler.h \
    Kafka_Communication_Handler.h \
    Client_Communication_Handler.h \
    Server_Master_Controller.h \
    Application_Activity_Logger.h \
    Client_Communication_Handler.h \
    Kafka_Producer_Handler.h \
    Client_Handler.h \
    Data_Receiver_Handler.h \
    Data_Segmentation_Handler.h \
#    Temp_Consumer.h

CONFIG += testcase

} else {

SOURCES += \
    Application_Activity_Logger.cpp \
    Client_Communication_Handler.cpp \
    Client_Handler.cpp \
    Config_Handler.cpp \
    Data_Receiver_Handler.cpp \
    Data_Segmentation_Handler.cpp \
    Kafka_Communication_Handler.cpp \
    Kafka_Producer_Handler.cpp \
    Queue_Manager.cpp \
    Server_Master_Controller.cpp \
    Unit_Test_cpp/Application_Activity_Logger_Testing.cpp \
    Unit_Test_cpp/Client_Communication_Handler_Testing.cpp \
    Unit_Test_cpp/Client_Handler_Testing.cpp \
    Unit_Test_cpp/Config_Handler_Testing.cpp \
    Unit_Test_cpp/Data_Receiver_Handler_Testing.cpp \
    Unit_Test_cpp/Data_Segmentation_Handler_Testing.cpp \
    Unit_Test_cpp/Kafka_Communication_Handler_Testing.cpp \
    Unit_Test_cpp/Kafka_Producer_Handler_Testing.cpp \
    Unit_Test_cpp/Server_Master_Controller_Testing.cpp \
    Temp_Consumer.cpp \
    main.cpp

HEADERS += \
    Application_Activity_Logger.h \
    Client_Communication_Handler.h \
    Client_Handler.h \
    Config_Handler.h \
    Data_Receiver_Handler.h \
    Data_Segmentation_Handler.h \
    Data_Structure.h \
    Kafka_Communication_Handler.h \
    Kafka_Producer_Handler.h \
    Server_Master_Controller.h \
    Temp_Consumer.h
}

# Default rules for deployment.
qnx: target.path = /tmp/$${TARGET}/bin
else: unix:!android: target.path = /opt/$${TARGET}/bin
!isEmpty(target.path): INSTALLS += target

# Add protobuf include and compiled sources
INCLUDEPATH += $$PWD/generated
SOURCES += $$PWD/generated/sensor_data.pb.cc

HEADERS += $$PWD/generated/sensor_data.pb.h \
    LockFreeQueue.h \
    timestampmerger.h



# Link protobuf library
LIBS += -lprotobuf

#TARGET = Config_Handler_Testing
#TARGET = Kafka_Communication_Handler_Testing
#TARGET = Client_Communication_Handler_Testing
#TARGET = Client_Handler_Testing
#TARGET = Data_Receiver_Handler_Testing
#TARGET = Data_Segmentation_Handler_Testing
#TARGET = Kafka_Producer_Handler_Testing
#TARGET = Server_Master_Controller_Testing

LIBS +=-lrdkafka++

DISTFILES += \
    proto/sensor_data.proto



