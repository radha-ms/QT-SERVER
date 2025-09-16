/**
 * This file provide a all kind fo unit testing functions for Conig_Handler module.
 */

#include <QtTest/QtTest>
#include <QFile>
#include <QTextStream>
#include "Config_Handler.h"
#include <fstream>

std::ofstream TestLog("Config_Handler_Test_Log.txt", std::ios::out);
class Config_Handler_test : public QObject
{
    Q_OBJECT
private slots:

    /**
     * ADDITIONAL TEST CASE DECLARATION
     */

    /**************************************VALID_FILE_PATHS**************************************************/
    //Checking filepath oriented testing
    void valid_serverfilepath();
    //checkig wrong filepath for server config
    void Invalid_serverFilepath();
    //Checking filepath oriented testing
    void valid_kafkafilepath();
    //checking wrong filepath for kafka config
    void Invalid_kafkafilepath();

    /**************************************LOGGER**************************************************/
    //checking that logger section is there in config file
    void Logger_section_Missing();
    //checking that log_level key is missing in LOGGER SECTION
    void Check_Log_Level_key_Missing();
    //checking that log_level value is missing in the file
    void Check_Log_Level_value_IsEmpty();
    //checking that log level is string
    void Check_Log_Level_value_is_String();
    //checking that log level is flaot
    void Check_Log_Level_value_is_float();
    //checking that log level is double
    void Check_Log_Level_value_is_double();
    //checking that log level is negative
    void Check_Log_Level_value_is_negative();

    /**************************************SERVER**************************************************/
    //checking that SERVER section is there in config file
    void Server_section_Missing();
    //checking that serverIp key is missing in config file
    void Check_serverIp_key_Missing();
    //check that serverPort key is missing in config file
    void Check_serverPort_key_Missing();
    //check that serverIp value is missing in config file
    void Check_serverIp_value_is_Empty();
    //check that serverIp value is string in config file
    void Check_serverIp_value_is_String();
    //check that serverIp value is invalid in config file
    void Check_serverIp_value_is_Invalid_Host();
    //check that serverip value is valid but zero in config file
    void Chcek_serverIp_value_is_validHost_Zero();
    //chek that serverPort value is missing in config file
    void Check_serverPort_value_is_Empty();
    //check that serverPort value is string in config file
    void Check_serverPort_value_is_String();
    //check that serverPort value is float in config file
    void Check_serverPort_value_is_float();
    //check that serverPort value is double in config file
    void Check_serverPort_value_is_double();
    //check that serverpotr value is crossed the max tcp port range
    void Check_serverPort_Crossed_Max_TCPRange();
    //check that serverport value is not negative
    void Check_serverPort_value_is_negative();

    /**************************************TEST**************************************************/
    //checking that SERVER section is there in config file
    void Test_section_Missing();
    //checking that serverIp key is missing in config file
    void Check_RefreshRate_key_Missing();
    //check that serverPort key is missing in config file
    void Check_ChannelID_SamplingRate_key_Missing();
    //check that RefreshRate value is empty in config file
    void check_RefreshRate_value_isEmpty();
    //check that RefreshRate value is string in config file
    void Check_RefreshRate_value_is_String();
    //check that RefreshRate value is float in config file
    void Check_RefreshRate_value_is_float();
    //check that RefreshRate value is double in config file
    void Check_RefreshRate_value_is_double();
    //check that RefreshRate value is crossed the max tcp port range
    void Check_RefreshRate_Crossed_Max_Range();
    //check that RefreshRate value is not negative
    void Check_RefreshRate_value_is_negative();
    //check that ChannelID_SamplingRate value is empty
    void Check_ChannelID_SamplingRate_isEmpty();
    //check that ChannelID_SamplingRate value is malformed
    void Check_ChannelID_SamplingRate_malformed();
    //check that channelID_SamplingRate value is invlaid
    void Check_ChannelID_SamplingRate_isInvalid();
    //check that ChannelID_SamplingRate value is float
    void Check_ChannelID_SamplingRate_isFloat();

    /**************************************KAFKA**************************************************/
    //Checking kafka section header is missing
    void Kafka_section_Missing();
    //check that KafkaURL key is missing
    void Check_KafkaURL_key_Missing();
    //check that Serializer key is missing
    void Check_Serializer_key_Missing();
    //check that NoOfAnalogTopic key is missing
    void Check_NoOfAnalogTopic_key_Missing();
    //check that NoOfDigitalTopic key is missing
    void Check_NoOfDigitalTopic_key_Missing();
    //check that kafkaURL value is empty
    void Check_KafkaURL_value_isEmpty();
    //check that kafkaURL value is invalid format
    void Check_KafkaURL_value_isInvalidFormat();
    //check that kafka IP Address is invalid type
    void Check_KafkaIP_value_isInvalidFormat();
    //check that kafka port is float
    void Check_Kafka_Port_value_isFloat();
    //check that kafka port is Double
    void Check_Kafka_port_value_isDouble();
    //check that kafka port is negative
    void Check_Kafka_port_value_isNegative();
    //check that kafka port is crossed max limit
    void Check_Kafka_port_value_isCrossed_Max();
    //check that kafka port is invalid characters
    void Check_Kafka_port_value_isInvalid_Chars();
    //check that serializer is empty
    void Check_Serializer_value_isEmpty();
    //check that serializer is numerical value
    void Check_Serializer_value_isNumeric();
    //check that serializer contains special characters
    void Check_Serializer_value_containsSPCHAR();
    //check that serializer is not expectedvalue
    void Check_Serializer_Value_isNot_Expected();
    //check that NoOfAnalogTopic value is empty
    void Check_NoOfAnalogtopic_value_isEmpty();
    //check that NoOfAnalogtopic value is string
    void Check_NoOfAnalogtopic_value_isString();
    //check that NoOfAnalogtopic value is float
    void Check_NoOfAnalogtopic_value_isFloat();
    //check that NoOfAnalogtopic value is double
    void Check_NoOfAnalogtopic_value_isDouble();
    //check that NoOfAnalogtopic value is crossed max range
    void Check_NoOfAnalogtopic_value_crossedMax();
    //check that NoOfAnalogtopic value is negative
    void Check_NoOfAnalogtopic_value_isNegative();
    //check that NoOfDigitalTopic value is empty
    void Check_NoOfDigitalTopic_value_isEmpty();
    //check that NoOfDigitalTopic value is string
    void Check_NoOfDigitalTopic_value_isString();
    //check that NoOfDigitalTopic value is float
    void Check_NoOfDigitalTopic_value_isFloat();
    //check that NoOfDigitalTopic value is double
    void Check_NoOfDigitalTopic_value_isDouble();
    //check that NoOfDigitalTopic value is crossed max range
    void Check_NoOfDigitalTopic_value_crossedMax();
    //check that NoOfDigitalTopic value is negative
    void Check_NoOfDigitalTopic_value_isNegative();

    /**************************************COMMUNICAION TOPIC**************************************************/
    //check that communication topic section is missing
    void Communication_topic_section_Missing();
    //check that topic name key is missing
    void check_Topic_name_key_Missing();
    //check that serializer key is missing
    void check_Serializer_key_Missing();
    //check that deserializer key is missing
    void check_DeSerializer_key_Missing();
    //check that topic name is empty
    void Check_Topic_Name_value_isEmpty();
    //check that topic name is invlaid type
    void Check_Topic_Name_value_isInvalid();
    //check that topic name is numerical
    void Check_Topic_Name_value_isNumeric();
    //check that serializer value is empty
    void Check_COMM_Serializer_value_isEmpty();
    //check that serializer value is invalid
    void Check_COMM_Serializer_value_isInvalid();
    //check that serializer value is numeric
    void Check_COMM_Serializer_value_isNumeric();
    //check that Deserializer value is empty
    void Check_COMM_DeSerializer_value_isEmpty();
    //check that Deserializer value is invalid
    void Check_COMM_DeSerializer_value_isInvalid();
    //check that Deserializer value is numeric
    void Check_COMM_DeSerializer_value_isNumeric();

    /**************************************ANALOG TOPICS**************************************************/
    //check that analog topic section is missing
    void Missing_Analog_Topic_section();
    //check that topic name key is missing
    void Check_Topic_Name_key_Missing();
    //check that min key is missing
    void Check_MinSampling_key_Missing();
    //check that max key is missing
    void Check_MaxSampling_key_Missing();
    //check that topic name value is empty
    void Check_AnalogTopicName_value_isEmpty();
    //check that minimum sampling rate value is missing
    void Check_AnalogMinSample_value_isEmpty();
    //check that max samplingarate value is missing
    void Check_AnalogMaxSample_value_isEmpty();
    //check that topic name is invalid type
    void Check_AnalogTopic_Name_isInvlaid();
    //check that AnalogMin sample Range crossed
    void Check_AnalogMinSample_Limitation();
    //check that AnalogMax sample Range crossed
    void Check_AnalogMaxSample_Limitation();
    //check that AnalogMax sample is less than min sample
    void Check_AnalogMaxsample_lessThan_Minsample();
    //check that analogMin sample is negative
    void Check_AnalogMinSample_isNegative();
    //check that analogMax sample is negative
    void Check_AnalogMaxSample_isNegative();
    /**************************************DIGITAL TOPICS**************************************************/

    //check that Digital topic section is missing
    void Missing_Digital_Topic_section();
    //check that topic name key is missing
    void Check_DigitalTopic_Name_key_Missing();
    //check that min key is missing
    void Check_SamplingRate_key_Missing();
    //check that topic name value is empty
    void Check_DigitalTopicName_value_isEmpty();
    //check that sampling rate value is missing
    void Check_SamplingRate_value_isEmpty();
    //check that topic name is invalid type
    void Check_DigitalTopic_Name_isInvlaid();
    //check that DigitalMin sample Range crossed
    void Check_DigitalSample_Limitation();
    //check that DigitalMin sample is negative
    void Check_DigitalSample_isNegative();

};




/**
 * @brief   Config_Handler_test::valid_serverfilepath
 * @param   None
 * @return  None
 * @details Checking server config filepath oriented testing
 */
void Config_Handler_test::valid_serverfilepath()
{
    QString correctpath = "/home/teclever/QT_Projects/DACS_Server_Application/DACS_Server_config.ini";
    Config_Handler handler;
    TestLog<<"============checking valid_server_filepath============="<<std::endl;
    TestLog<<"input correct file path :-"<<correctpath.toStdString()<<std::endl;
    int result = handler.init_Server(correctpath);
    activityLog<<"################################################"<<endl;
    TestLog<<"expected output :- 1"<<std::endl;
    TestLog<<"actual output :- "<<result<<std::endl;
    QCOMPARE(result, 1);
}

/**
 * @brief   Config_Handler_test::Invalid_serverFilepath
 * @param   None
 * @return  None
 * @details Checking server config filepath oriented testing with invalid filename
 */
void Config_Handler_test::Invalid_serverFilepath(){

    QString wrongpath = "/home/teclever/QT_Projects/DACS_Server_Application/DACS_Invalid_ServerFile_config.ini";
    Config_Handler handler1;
    TestLog<<"============Checking Invalid_server_filepath============="<<std::endl;
    TestLog<<"input wrong file path :-"<<wrongpath.toStdString()<<std::endl;
    int result1 = handler1.init_Server(wrongpath);
    activityLog<<"################################################"<<endl;
    TestLog<<"expected output :- 0"<<std::endl;
    TestLog<<"actual output :- "<<result1<<std::endl;
    QCOMPARE(result1, 0);
}


/**
 * @brief   Config_Handler_test::valid_kafkafilepath
 * @param   None
 * @return  None
 * @details Checking kafka config filepath oriented testing
 */
void Config_Handler_test::valid_kafkafilepath(){
    QString kafka_path = "/home/teclever/QT_Projects/DACS_Server_Application/DACS_Kafka_config.ini";
    Config_Handler handler;
    TestLog<<"============valid_kafka_filepath============="<<std::endl;
    TestLog<<"input correct file path :-"<<kafka_path.toStdString()<<std::endl;
    int result = handler.init_Kafka(kafka_path);
    TestLog<<"expected output :- 1"<<std::endl;
    TestLog<<"actual output :- "<<result<<std::endl;
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 1);
}



/**
 * @brief   Config_Handler_test::Invalid_kafkafilepath
 * @param   None
 * @return  None
 * @details Checking kafka config filepath oriented testing with invalid filename
 */
void Config_Handler_test::Invalid_kafkafilepath(){
    QString kafka_path = "/home/teclever/QT_Projects/DACS_Server_Application/DACS_Invalid_KafkaFile_config.ini";
    Config_Handler handler;
    TestLog<<"============valid_kafka_filepath============="<<std::endl;
    TestLog<<"input wrong file path :- "<<kafka_path.toStdString()<<std::endl;
    int result = handler.init_Kafka(kafka_path);
    TestLog<<"expected output :- 0"<<std::endl;
    TestLog<<"actual output :- "<<result<<std::endl;
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Check_logger_section
 * @param   None
 * @return  None
 * @details This function is used to testing the Missing LOGGER section header in the config file
 */
void Config_Handler_test::Logger_section_Missing(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/logger/Missing_Logger_section_Header.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============valid_LOGGER section_header In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Check_logger_section
 * @param   None
 * @return  None
 * @details This function is used to testing the Missing Log_Level key in the config file
 */
void Config_Handler_test::Check_Log_Level_key_Missing(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/logger/Missing_Log_Level_key.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============valid_Log_Level key In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief   Config_Handler_test::Check_Log_Level_value_IsEmpty
 * @param   None
 * @return  None
 * @details This function is used to check the log_level value is empty or not
 */
void Config_Handler_test::Check_Log_Level_value_IsEmpty(){

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/logger/Empty_Log_Level_value.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Empty_Log_Level value In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief   Config_Handler_test::Check_Log_Level_value_IsEmpty
 * @param   None
 * @return  None
 * @details This function is used to check the log_level value is string
 */
void Config_Handler_test::Check_Log_Level_value_is_String(){

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/logger/Log_Level_value_in_String.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_Log_Level value is String In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief   Config_Handler_test::Check_Log_Level_value_IsEmpty
 * @param   None
 * @return  None
 * @details This function is used to check the log_level value is float
 */
void Config_Handler_test::Check_Log_Level_value_is_float(){

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/logger/Log_Level_value_in_float.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_Log_Level value is float In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief   Config_Handler_test::Check_Log_Level_value_IsEmpty
 * @param   None
 * @return  None
 * @details This function is used to check the log_level value is double
 */
void Config_Handler_test::Check_Log_Level_value_is_double(){

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/logger/Log_Level_value_in_double.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_Log_Level value is double In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief   Config_Handler_test::Check_Log_Level_value_IsEmpty
 * @param   None
 * @return  None
 * @details This function is used to check the log_level value is negative
 */
void Config_Handler_test::Check_Log_Level_value_is_negative(){

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/logger/Log_Level_value_in_negative.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_Log_Level value is negative In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Server_section_Missing
 */
void Config_Handler_test::Server_section_Missing(){
    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/Missing_SERVER_section_Header.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER section header In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_serverIp_key_Missing
 */
void Config_Handler_test::Check_serverIp_key_Missing(){
    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/Missing_serverIP_key.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVERIP key In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_serverPort_key_Missing
 */
void Config_Handler_test::Check_serverPort_key_Missing(){
    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/Missing_serverPort_key.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_PORT key In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_serverIp_value_is_Empty
 */
void Config_Handler_test::Check_serverIp_value_is_Empty(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/serverIP_value_isEmpty.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_IP value is empty In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_serverIp_value_is_String
 */
void Config_Handler_test::Check_serverIp_value_is_String(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/serverIP_value_isString.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_IP value is String In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_serverIp_value_is_Invalid ip format
 */
void Config_Handler_test::Check_serverIp_value_is_Invalid_Host(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/serverIP_value_is_invalid.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_IP value is invalid ip address format In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Chcek_serverIp_value_is_validHost_Zero
 */
void Config_Handler_test::Chcek_serverIp_value_is_validHost_Zero(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/serverIP_value_is_validZero.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_IP value is valid but 0.0.0.0 format In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_serverPort_value_is_Empty
 */
void Config_Handler_test::Check_serverPort_value_is_Empty(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/serverPort_value_isEmpty.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_PORT value is missing In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Check_serverPort_value_is_String
 */
void Config_Handler_test::Check_serverPort_value_is_String(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/serverPort_value_isString.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_PORT value is string In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_serverPort_value_is_float
 */
void Config_Handler_test::Check_serverPort_value_is_float(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/serverPort_value_isFloat.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_PORT value is float In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_serverPort_value_is_double
 */
void Config_Handler_test::Check_serverPort_value_is_double(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/serverPort_value_isDouble.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_PORT value is double In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_serverPort_Crossed_Max_TCPRange
 */
void Config_Handler_test::Check_serverPort_Crossed_Max_TCPRange(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/serverPort_value_MAX_Range.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_PORT value is out of Range In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_serverPort_value_is_negative
 */
void Config_Handler_test::Check_serverPort_value_is_negative(){
    QString path = "/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/server/serverPort_value_is_negative.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;
    activityLog<<"==============Valid_SERVER_PORT value is neagative In Config==========="<<endl;
    int result = handler.Read_Server_Log_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Test_section_Missing
 */
void Config_Handler_test::Test_section_Missing() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/Missing_TEST_section_Header.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_TEST_section_missing in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_RefreshRate_key_Missing
 */
void Config_Handler_test::Check_RefreshRate_key_Missing() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/Missing_RefreshRate_key.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_RefreshRate key missing in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_ChannelID_SamplingRate_key_Missing
 */
void Config_Handler_test::Check_ChannelID_SamplingRate_key_Missing() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/Missing_ChannelID_SamplingRate_key.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_ChannelID_SamplingRate key missing in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::check_RefreshRate_value_isEmpty
 */
void Config_Handler_test::check_RefreshRate_value_isEmpty() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/RefreshRate_value_isEmpty.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_RefreshRate_value_isEmpty in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_RefreshRate_value_is_String
 */
void Config_Handler_test::Check_RefreshRate_value_is_String() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/RefreshRate_value_isString.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_RefreshRate_value_isString in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_RefreshRate_value_is_float
 */
void Config_Handler_test::Check_RefreshRate_value_is_float() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/RefreshRate_value_isFloat.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_RefreshRate_value_isFloat in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_RefreshRate_value_is_double
 */
void Config_Handler_test::Check_RefreshRate_value_is_double() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/RefreshRate_value_isDouble.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_RefreshRate_value_isDouble in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_RefreshRate_Crossed_Max_Range
 */
void Config_Handler_test::Check_RefreshRate_Crossed_Max_Range() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/RefreshRate_value_crossed_max.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_RefreshRate_value_isNegative in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_RefreshRate_value_is_negative
 */
void Config_Handler_test::Check_RefreshRate_value_is_negative() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/RefreshRate_value_isNegative.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_RefreshRate_value_isNegative in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_ChannelID_SamplingRate_isEmpty
 */
void Config_Handler_test::Check_ChannelID_SamplingRate_isEmpty() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/ChannelID_SamplingRate_value_isEmpty.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_ChannelID_SamplingRate_value_isEmpty in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_ChannelID_SamplingRate_malformed
 */
void Config_Handler_test::Check_ChannelID_SamplingRate_malformed() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/ChannelID_SamplingRate_value_isMalformed.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_ChannelID_SamplingRate_value_isMalformed in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_ChannelID_SamplingRate_isInvalid
 */
void Config_Handler_test::Check_ChannelID_SamplingRate_isInvalid() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/ChannelID_SamplingRate_value_isInvalid.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_ChannelID_SamplingRate_value_isInvalid in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_ChannelID_SamplingRate_isFloat
 */
void Config_Handler_test::Check_ChannelID_SamplingRate_isFloat() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/test/ChannelID_SamplingRate_value_isFloat.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_ChannelID_SamplingRate_value_isNotInteger in config==========="<<endl;
    int result = handler.Read_Test_config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Kafka_section_Missing
 */
void Config_Handler_test::Kafka_section_Missing() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Missing_KAFKA_section_header.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid_KAFKA section header is missing in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_KafkaURL_key_Missing
 */
void Config_Handler_test::Check_KafkaURL_key_Missing() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Missing_KafkaURL_key.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid KafkaURL key is missing in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Serializer_key_Missing
 */
void Config_Handler_test::Check_Serializer_key_Missing() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Missing_Serializer_key.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Serializer key is missing in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfAnalogTopic_key_Missing
 */
void Config_Handler_test::Check_NoOfAnalogTopic_key_Missing() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Missing_NoOfAnalogTopic_key.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfAnalogTopic key is missing in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfDigitalTopic_key_Missing
 */
void Config_Handler_test::Check_NoOfDigitalTopic_key_Missing() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Missing_NoOfDigitalTopic_key.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfDigitalTopic key is missing in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_KafkaURL_value_isEmpty
 */
void Config_Handler_test::Check_KafkaURL_value_isEmpty() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Kafka_URL_value_isEmpty.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid KafkaURL value is empty in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_KafkaURL_value_isInvalidFormat
 */
void Config_Handler_test::Check_KafkaURL_value_isInvalidFormat() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Kafka_URL_value_isInvalidFormat.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid KafkaURL value is Invalidtype in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_KafkaIP_value_isInvalidFormat
 */
void Config_Handler_test::Check_KafkaIP_value_isInvalidFormat() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Kafka_IP_value_isInvalidFormat.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid KafkaURL IP ADDRESS is Invalidtype in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Kafka_Port_value_isFloat
 */
void Config_Handler_test::Check_Kafka_Port_value_isFloat() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Kafka_PORT_value_isFloat.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid KafkaURL PORT is Float in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Kafka_port_value_isDouble
 */
void Config_Handler_test::Check_Kafka_port_value_isDouble() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Kafka_PORT_value_isDouble.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid KafkaURL PORT is Double in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Kafka_port_value_isNegative
 */
void Config_Handler_test::Check_Kafka_port_value_isNegative() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Kafka_PORT_value_isNegative.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid KafkaURL PORT is negative in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Kafka_port_value_isCrossed_Max
 */
void Config_Handler_test::Check_Kafka_port_value_isCrossed_Max() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Kafka_PORT_value_isCrossed_Max.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid KafkaURL PORT is crossed max range in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Kafka_port_value_isInvalid_Chars
 */
void Config_Handler_test::Check_Kafka_port_value_isInvalid_Chars() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Kafka_PORT_value_isInvalid_chars.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid KafkaURL PORT is invalid chars in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Serializer_value_isEmpty
 */
void Config_Handler_test::Check_Serializer_value_isEmpty() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Serializer_value_isEmpty.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid SERIALIZER is empty in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Check_Serializer_value_isNumeric
 */
void Config_Handler_test::Check_Serializer_value_isNumeric() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Serializer_value_isNumeric.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid SERIALIZER is Numeric in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Serializer_value_containsSPCHAR
 */
void Config_Handler_test::Check_Serializer_value_containsSPCHAR() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Serializer_value_containsSPCHAR.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid SERIALIZER contains special CHAR in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Serializer_Value_isNot_Expected
 */
void Config_Handler_test::Check_Serializer_Value_isNot_Expected() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Serializer_value_isInvalid.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid SERIALIZER is Numeric in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfAnalogtopic_value_isEmpty
 */
void Config_Handler_test::Check_NoOfAnalogtopic_value_isEmpty() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Missing_NoOfAnalog_topic_value.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfAnalogtopic_value_isEmpty in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfAnalogtopic_value_isString
 */
void Config_Handler_test::Check_NoOfAnalogtopic_value_isString() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/NoOfAnalog_topic_value_isString.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfAnalogtopic_value_isString in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfAnalogtopic_value_isFloat
 */
void Config_Handler_test::Check_NoOfAnalogtopic_value_isFloat() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/NoOfAnalog_topic_value_isfloat.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfAnalogtopic_value_isFloat in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfAnalogtopic_value_isDouble
 */
void Config_Handler_test::Check_NoOfAnalogtopic_value_isDouble() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/NoOfAnalog_topic_value_isDouble.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfAnalogtopic_value_isDouble in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfAnalogtopic_value_crossedMax
 */
void Config_Handler_test::Check_NoOfAnalogtopic_value_crossedMax() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/NoOfAnalog_topic_value_isCrossedMAX.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfAnalogtopic_value_isCrossed max limit in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfAnalogtopic_value_isNegative
 */
void Config_Handler_test::Check_NoOfAnalogtopic_value_isNegative() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/NoOfAnalog_topic_value_isNegative.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfAnalogtopic_value_isNegative in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Check_NoOfDigitalTopic_value_isEmpty
 */
void Config_Handler_test::Check_NoOfDigitalTopic_value_isEmpty() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/Missing_NoOfDigital_topic_value.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfDigitalTopic_value_isEmpty in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfDigitalTopic_value_isString
 */
void Config_Handler_test::Check_NoOfDigitalTopic_value_isString() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/NoOfDigital_topic_value_isString.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfDigitalTopic_value_isString in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfDigitalTopic_value_isFloat
 */
void Config_Handler_test::Check_NoOfDigitalTopic_value_isFloat() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/NoOfDigital_topic_value_isfloat.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfDigitalTopic_value_isFloat in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfDigitalTopic_value_isDouble
 */
void Config_Handler_test::Check_NoOfDigitalTopic_value_isDouble() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/NoOfDigital_topic_value_isDouble.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfDigitalTopic_value_isDouble in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfDigitalTopic_value_crossedMax
 */
void Config_Handler_test::Check_NoOfDigitalTopic_value_crossedMax() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/NoOfDigital_topic_value_isCrossedMAX.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfDigitalTopic_value_isCrossed max limit in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_NoOfDigitalTopic_value_isNegative
 */
void Config_Handler_test::Check_NoOfDigitalTopic_value_isNegative() {


    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/kafka/NoOfDigital_topic_value_isNegative.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid NoOfDigitalTopic_value_isNegative in config==========="<<endl;
    int result = handler.Read_Kafka_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Communication_topic_section_Missing
 */
void Config_Handler_test::Communication_topic_section_Missing() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/Missing_Communication_Topic_section.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Communication_topic_section_Missing in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::check_Topic_name_key_Missing
 */
void Config_Handler_test::check_Topic_name_key_Missing() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/Topic_Name_key_missing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Topic_Name_key_missing in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::check_Serializer_key_Missing
 */
void Config_Handler_test::check_Serializer_key_Missing() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/Serializer_key_missing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Serializer_key_Missing in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::check_DeSerializer_key_Missing
 */
void Config_Handler_test::check_DeSerializer_key_Missing() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/DeSerializer_key_missing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid DeSerializer_key_Missing in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Topic_Name_value_isEmpty
 */
void Config_Handler_test::Check_Topic_Name_value_isEmpty() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/Topic_Name_value_isEmpty.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Check_Topic_Name_value_isEmpty in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Topic_Name_value_isInvalid
 */
void Config_Handler_test::Check_Topic_Name_value_isInvalid() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/Topic_Name_value_isInvalid.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Check_Topic_Name_value_isInvalid in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_Topic_Name_value_isNumeric
 */
void Config_Handler_test::Check_Topic_Name_value_isNumeric() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/Topic_Name_value_isNumeric.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Check_Topic_Name_value_isNumeric in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_COMM_Serializer_value_isEmpty
 */
void Config_Handler_test::Check_COMM_Serializer_value_isEmpty() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/Serializer_value_isEmpty.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Check_COMM_Serializer_value_isEmpty in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_COMM_DeSerializer_value_isEmpty
 */
void Config_Handler_test::Check_COMM_DeSerializer_value_isEmpty() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/DeSerializer_value_isEmpty.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Check_COMM_DeSerializer_value_isEmpty in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Check_COMM_DeSerializer_value_isInvalid
 */
void Config_Handler_test::Check_COMM_DeSerializer_value_isInvalid() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/DeSerializer_value_isInvalid.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Check_COMM_DeSerializer_value_isInvalid in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}



/**
 * @brief Config_Handler_test::Check_COMM_DeSerializer_value_isNumeric
 */
void Config_Handler_test::Check_COMM_DeSerializer_value_isNumeric() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/DeSerializer_value_isNumeric.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Check_COMM_DeSerializer_value_isNumeric in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Check_COMM_Serializer_value_isInvalid
 */
void Config_Handler_test::Check_COMM_Serializer_value_isInvalid() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/Serializer_value_isInvalid.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Check_COMM_Serializer_value_isInvalid in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_COMM_Serializer_value_isNumeric
 */
void Config_Handler_test::Check_COMM_Serializer_value_isNumeric() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/communication_topic/Serializer_value_isNumeric.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Check_COMM_Serializer_value_isNumeric in config==========="<<endl;
    int result = handler.Read_Communication_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Missing_Analog_Topic_section
 */
void Config_Handler_test::Missing_Analog_Topic_section() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/Analogtopic_section_Missing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Analogtopic_section_Missing in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


/**
 * @brief Config_Handler_test::Check_Topic_Name_key_Missing
 */
void Config_Handler_test::Check_Topic_Name_key_Missing() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/Topic_Name_key_isMissing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Topic_Name_key_Missing in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_MinSampling_key_Missing
 */
void Config_Handler_test::Check_MinSampling_key_Missing() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/MinSampling_key_isMissing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid MinSampling_key_Missing in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_MaxSampling_key_Missing
 */
void Config_Handler_test::Check_MaxSampling_key_Missing() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/MaxSampling_key_isMissing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid MaxSampling_key_Missing in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_AnalogTopicName_value_isEmpty
 */
void Config_Handler_test::Check_AnalogTopicName_value_isEmpty() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/Topic_Name_value_isMissing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid TopicName value Missing in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_AnalogMinSample_value_isEmpty
 */
void Config_Handler_test::Check_AnalogMinSample_value_isEmpty(){

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/MinSampling_value_isMissing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Min sampling value Missing in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_AnalogMaxSample_value_isEmpty
 */
void Config_Handler_test::Check_AnalogMaxSample_value_isEmpty(){

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/MaxSampling_value_isMissing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Max sampling value Missing in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_AnalogTopic_Name_isInvlaid
 */
void Config_Handler_test::Check_AnalogTopic_Name_isInvlaid(){

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/Topic_Name_value_isInvalid.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid analog topicname value Invalid in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_AnalogMinSample_Limitation
 */
void Config_Handler_test::Check_AnalogMinSample_Limitation(){

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/MinSampling_value_Range.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid analog Min Sampling_value limitation in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_AnalogMaxSample_Limitation
 */
void Config_Handler_test::Check_AnalogMaxSample_Limitation(){

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/MaxSampling_value_Range.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid analog Max Sampling_value limitation in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_AnalogMaxsample_lessThan_Minsample
 */
void Config_Handler_test::Check_AnalogMaxsample_lessThan_Minsample() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/MaxSampling_lessThan_MinSampling.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid analog MaxSampling_lessThan_MinSampling in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_AnalogMinSample_isNegative
 */
void Config_Handler_test::Check_AnalogMinSample_isNegative() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/MinSampling_value_isNegative.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid AnalogMinSample_isNegative in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_AnalogMaxSample_isNegative
 */
void Config_Handler_test::Check_AnalogMaxSample_isNegative() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Analog_topics/MaxSampling_value_isNegative.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid AnalogMaxSample_isNegative in config==========="<<endl;
    int result = handler.Read_Analog_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Missing_Digital_Topic_section
 */
void Config_Handler_test::Missing_Digital_Topic_section() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Digital_topics/Digitaltopic_section_Missing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Digitaltopic section Missing in config==========="<<endl;
    int result = handler.Read_Digital_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_DigitalTopic_Name_key_Missing
 */
void Config_Handler_test::Check_DigitalTopic_Name_key_Missing() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Digital_topics/DigitalTopicName_key_Missing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid DigitalTopicName key Missing in config==========="<<endl;
    int result = handler.Read_Digital_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_SamplingRate_key_Missing
 */
void Config_Handler_test::Check_SamplingRate_key_Missing() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Digital_topics/Digitalsample_key_Missing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Digitalsample key Missing in config==========="<<endl;
    int result = handler.Read_Digital_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_DigitalTopicName_value_isEmpty
 */
void Config_Handler_test::Check_DigitalTopicName_value_isEmpty() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Digital_topics/DigitalTopicName_value_Missing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid DigitalTopicName value Missing in config==========="<<endl;
    int result = handler.Read_Digital_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_SamplingRate_value_isEmpty
 */
void Config_Handler_test::Check_SamplingRate_value_isEmpty() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Digital_topics/Digitalsample_value_Missing.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Digitalsample value Missing in config==========="<<endl;
    int result = handler.Read_Digital_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_DigitalTopic_Name_isInvlaid
 */
void Config_Handler_test::Check_DigitalTopic_Name_isInvlaid() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Digital_topics/DigitalTopicName_value_isInvalid.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid DigitalTopicName value Invalid in config==========="<<endl;
    int result = handler.Read_Digital_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_DigitalSample_Limitation
 */
void Config_Handler_test::Check_DigitalSample_Limitation() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Digital_topics/Digitalsample_value_Limitation.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid Digital MaxSample_Range in config==========="<<endl;
    int result = handler.Read_Digital_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}

/**
 * @brief Config_Handler_test::Check_DigitalSample_isNegative
 */
void Config_Handler_test::Check_DigitalSample_isNegative() {

    QString path ="/home/teclever/QT_Projects/DACS_Server_Application/Ini_Files/Digital_topics/Digitalsample_value_isNegative.ini";
    QSettings settings(path, QSettings::IniFormat);
    Config_Handler handler;

    activityLog<<"==============Valid digital sampling isNegative in config==========="<<endl;
    int result = handler.Read_Digital_Topic_Config(&settings);
    activityLog<<"################################################"<<endl;
    QCOMPARE(result, 0);
}


//QTEST_MAIN(Config_Handler_test)
#include "Config_Handler_Testing.moc"
