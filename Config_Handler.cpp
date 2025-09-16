/**
*****************************************************************************************************************************************
* @file    Config_Handler.h
* @author  karthikeya.sr
* @version V1.0
* @date    02-May-2025
* @brief   This file provides all reading Configuration details from the config file and storing in appropriate variables.
*
* @details 1.Read the filepath
*          2.Open the .ini file
*          3.Read the data from below section headers and store it in respective variables
*            [SERVER,KAFKA,TEST,LOGGER]
*          4.Emit the signal to server_master_controller with success or failure command.
*
******************************************************************************************************************************************/

/*
 *Header file which is included and used
 */
#include "Config_Handler.h"


/**
 * @brief   Config_Handler constrcutor to Init the file and start the thread to read.
 * @param   parent
 * @return  None
 * @details Initiate filepath
 *          Check the filepath is exist or not
 *          Emit the signal if file is not exist
 *          Open the file to read
 *          Emit the signal if cannot open the file
 *          Start the thread to read data once the file is opened.
 */
Config_Handler::Config_Handler(QObject *parent) : QObject(parent){}

/**
 * @brief   Init function to Init the file and call functions to read.
 * @param   Server file path
 * @return  '1' if successfully executed '0' if any error occured
 * @details Initiate filepath
 *          Check the filepath is exist or not
 *          Emit the signal if file is not exist
 *          Open the file to read
 *          Emit the signal if cannot open the file
 *          Call functions to read data once the file is opened.
 */
int Config_Handler::init_Server(QString server_filePath)
{
    //to store the returned value from below functions
    int server_returns, test_returns;

    activityLog<<"===========================INIT SERVER CONFIG=================================";


    //checking the filepath
    if (QFile::exists(server_filePath)) {

        activityLog<<"INI file exists at:" + server_filePath<<"\n";
        activityLog<< "********************************************************************************";

    }
    else {
        //emit the signal to master class slot to handle the error
        emit configLoaded("INI file does not exist at:" +server_filePath,0);
        return 0;
    }

    // opening file to read the file
    QSettings *config=new QSettings(server_filePath,QSettings::IniFormat);
    QFile configFile(server_filePath);

    //check that file can open or not
    if (configFile.open(QIODevice::ReadOnly | QIODevice::Text)) {
        activityLog<<"Opened config file for reading:" + server_filePath;
        activityLog<<"********************************************************************************";
        activityLog<<"***********************************************End of INIT SERVER CONFIG******************************************************""\n";


        // call the Read_Server_Log_Config function with Qsetting object if the file is opened to read server & logger section data
        server_returns = Read_Server_Log_Config(config);
        // call the Read_Test_config function with Qsetting object if the file is opened to read test section data
        test_returns = Read_Test_config(config);

        //check the returned value is success or failure
        if(server_returns!=1 || test_returns !=1){
            //emit the signal to master class slot to handle the failure message
            emit configLoaded("failed to parsing data from server configuration file", 0);
            return 0;
        }
        return 1;
    }
    else {
        //emit the signal to master class slot to handle the error
        emit configLoaded("Cannot open config file for reading: " +server_filePath,0);
        return 0;
    }
}

/**
 * @brief   Init function to Init the file and call functions to read.
 * @param   Kafka file path
 * @return  '1' if successfully executed '0' if any error occured
 * @details Initiate filepath
 *          Check the filepath is exist or not
 *          Emit the signal if file is not exist
 *          Open the file to read
 *          Emit the signal if cannot open the file
 *          Call functions to read data once the file is opened.
 */
int Config_Handler::init_Kafka(QString Kafka_filePath){

    activityLog<<"=========================INIT KAFKA CONFIG==========================";

    //to store the below functions returned value
    int kafka_returns,comm_returns,analog_returns,digital_returns;

    //checking the filepath
    if (QFile::exists(Kafka_filePath)) {
        activityLog<<"INI file exists at:" + Kafka_filePath;
        activityLog<< "********************************************************************************";

    }
    else {
        activityLog<<"INI file does not exist at:" +Kafka_filePath;
        activityLog<<"********************************************************************************";
        //emit the signal to master class slot to handler the error
        emit configLoaded("INI file does not exist at:" +Kafka_filePath,0);
        return 0;
    }

    // opening file to read the file
    QSettings *kafka_config=new QSettings(Kafka_filePath,QSettings::IniFormat);
    QFile kafkaconfigFile(Kafka_filePath);

    //check that file can open or not
    if (kafkaconfigFile.open(QIODevice::ReadOnly | QIODevice::Text)) {
        activityLog<<"Opened config file for reading:" + Kafka_filePath;
        activityLog<<"********************************************************************************";
        activityLog<<"***********************************************End of INIT KAFKA CONFIG******************************************************""\n";

        // call the Read_Kafka_Config function with Qsetting object if the file is opened to Kafka section data
        kafka_returns=  Read_Kafka_Config(kafka_config);
        // call the Read_Communication_Config function with Qsetting object if the file is opened to communication topic section data
        comm_returns = Read_Communication_Config(kafka_config);
        // call the Read_Analog_Topic_Config function with Qsetting object if the file is opened to analog topic section data
        analog_returns = Read_Analog_Topic_Config(kafka_config);
        // call the Read_Digital_Topic_Config function with Qsetting object if the file is opened to Digital topic test section data
        digital_returns =  Read_Digital_Topic_Config(kafka_config);

        if(kafka_returns!=1 || comm_returns!=1 || analog_returns!=1 || digital_returns !=1){
            emit configLoaded("failed to read kafka configuration file", 0);
        }else {
            emit configLoaded("Server and Kafka file parsing were successful; start all modules",1);
        }
    }
    else {
        activityLog<<"Cannot open config file for reading:" +Kafka_filePath;
        activityLog<<"********************************************************************************";
        //emit the signal to master class slot to handler the error
        emit configLoaded("Cannot open config file for reading:" +Kafka_filePath,0);
        return 0;
    }

    return 1;

}


/**
 * @brief   This function is used to read and store SERVER and LOGGER data into the appropriate variables.
 * @param   QSetting object, access for the file
 * @retval  '1' if successfully executed '0' if any error occured
 * @details Read the data from server section Header on this file
 *          And store it in respective variables
 */
int Config_Handler::Read_Server_Log_Config(QSettings *object){

    activityLog<<"/======================LOGGER==================================";

    // Check if "LOGGER" group exists (SECTION)
    QStringList allGroups = object->childGroups();
    if (!allGroups.contains("LOGGER")) {
        activityLog << "warning: 'LOGGER' section is missing in the config file";
        return 0;
    }

    // Read LOGGER section: log level setting.
    object->beginGroup("LOGGER");

    // Check if "Log_Level" key exists
    QStringList allKeys = object->childKeys();
    if (!allKeys.contains("Log_Level")) {
        activityLog << "warning: 'Log_Level' key is missing in LOGGER section";
        object->endGroup();
        return 0;
    }

    // Retrieve the raw value
    QString logStr = object->value("Log_Level").toString().trimmed();

    // Check if value is empty
    if (logStr.isEmpty()) {
        activityLog << "warning: Log_Level is empty";
        object->endGroup();
        return 0;
    }

    // Check if value is a valid integer (not float or non-numeric)
    bool isInt = false;
    int logLevel = logStr.toInt(&isInt);
    if (!isInt) {
        activityLog << "warning: Log_Level must be an integer value, actual: " << logStr;
        object->endGroup();
        return 0;
    }

    //Check if value is negative
    if(logLevel<0){
        activityLog << "warning: Log_Level must be positive only, actual: " << logLevel;
        object->endGroup();
        return 0;
    }

    // Check if value is exactly 1 or 2
    if (logLevel == 0 || logLevel > 2) {
        activityLog << "warning: Log_Level must be 1 or 2 only, actual: " << logLevel;
        object->endGroup();
        return 0;
    }

    //setting the Log_Level
    setLogLevel(logLevel);

    //check the loglevel is 2 to write the debug statement into the log file
    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"log_level "<<getLogLevel();

    object->endGroup();
    activityLog<<"*************************End of log_Config********************************************""\n";


    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"==============================SERVER====================================";

    //Check that server section is there in the config file
    if (!allGroups.contains("SERVER")) {
        activityLog << "warning: 'SERVER' section is missing in the config file";
        return 0;
    }

    // Read SERVER section: IP and Port settings.
    object->beginGroup("SERVER");

    // Check if "serverIp" key exists
    QStringList allKeys_inserver = object->childKeys();
    if (!allKeys_inserver.contains("ServerIP")) {
        activityLog << "warning: 'serverIp' key is missing in SERVER section";
        object->endGroup();
        return 0;
    }
    //check if "serverPort" key exists
    if (!allKeys_inserver.contains("ServerPort")) {
        activityLog << "warning: 'serverPort' key is missing in SERVER section";
        object->endGroup();
        return 0;
    }

    //create local object to store the parsing value
    SERVER server;


    // Regular expression for strict IPv4 format validation
    QRegularExpression ipv4Strict(R"(^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$)");

    //parsing the value
    QString temp_server_IP = object->value("ServerIP").toString();

    // Validate ServerIp
    if(temp_server_IP == "0.0.0.0")
    {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: ServerIp cannot be 0.0.0.0";
        object->endGroup();
        return 0;
    }

    //validate the value is in IP format ot not
    if (!ipv4Strict.match(temp_server_IP).hasMatch() || QHostAddress(temp_server_IP).isNull()) {
        if (getLogLevel() >= LOGLEVELINFO) {
            activityLog << "warning: server_IP is not a valid IP address "<<temp_server_IP;
            object->endGroup();
            return 0;
        }
    }else
    {
        //Assing the IpAdd to the structure
        server.ServerIP = QHostAddress(temp_server_IP);
    }


    //check the loglevel is 2 to write the debug statement into the log file
    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"serverIP : "<<server.ServerIP .toString();


    //parsing the serverPort
    int tempport = object->value("ServerPort").toInt();

    //convert it as String to checking puropse
    QString port  =QString::number(tempport).trimmed();

    //Check that port is empty
    if(port.isEmpty()){
        activityLog << "warning: serverPort is empty: " << port;
        object->endGroup();
        return 0;
    }

    // Check if value is a valid integer (not float or non-numeric)
    bool isInt1 = false;
    quint32 Realport = port.toInt(&isInt1);
    if (!isInt1) {
        activityLog << "warning: serverPort must be an integer value, actual: " << port;
        object->endGroup();
        return 0;
    }

    // check Detect clearly invalid types like alphabets-only strings
    if (port.contains(QRegularExpression("^[a-zA-Z\\s]+$"))) {
        activityLog << "warning: serverPort appears to be a non-numeric string: " << port;
        object->endGroup();
        return 0;
    }

    //check the port is less than 0 or negative sign
    if(Realport<0){
        activityLog << "warning: serverPort is negative value (less than zero) : " << Realport;
        object->endGroup();
        return 0;
    }

    //check the port is crossed max range of tcp
    if( Realport>65535){
        activityLog << "warning:  serverPort should be in a valid TCP/UDP port range (1–65535): " << Realport;
        object->endGroup();
        return 0;
    }


    // Validate serverPort
    if (Realport == 0)//SERVER PORT should be in a valid TCP/UDP port range (1–65535).
    {
        if (getLogLevel() >= LOGLEVELINFO){
            activityLog << "warning : serverport is zero:"<<Realport;
            return 0;
        }
    }

    //setting the serverPort
    server.ServerPort = Realport;

    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"serverPort : "<<server.ServerPort;


    //set the readed value as a structure for server
    setSERVERdetail(server);

    //end of server group
    object->endGroup();

    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"*************************End of Server_Config_method********************************************""\n";


    return 1;
}

/**
 * @brief   This function is used to read and store TEST data into the appropriate variables.
 * @param   QSetting object, access for the file
 * @retval  '1' if successfully executed '0' if any error occured
 * @details Read the data from test section Header on this file
 *          And store it in respective variables
 */
int Config_Handler::Read_Test_config(QSettings *object){

    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"======================TEST=============================";

    // Check if "TEST" group exists (SECTION)
    QStringList allGroups = object->childGroups();
    if (!allGroups.contains("TEST")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'TEST' section is missing in the config file";
        return 0;
    }

    // Read TEST section: refresh rate, parse ChannelID and SamplingRate lists into channelId→samplingRate map.
    object->beginGroup("TEST");

    // Check if "RefreshRate" key exists
    QStringList allKeys = object->childKeys();
    if (!allKeys.contains("RefreshRate")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'RefreshRate' key is missing in TEST section";
        object->endGroup();
        return 0;
    }

    // Check if "ChannelID_SamplingRate" key exists
    if (!allKeys.contains("ChannelID_SamplingRate")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'ChannelID_SamplingRate' key is missing in TEST section";
        object->endGroup();
        return 0;
    }


    //create a TEST structure object to store the test details
    TEST test_config;

    //parsing the RefreshRate
    QString temp_refrehRate  =object->value("RefreshRate").toString();

    //check is empty
    if(temp_refrehRate.isEmpty()){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : refresh rate is empty "<<temp_refrehRate;
        object->endGroup();
        return 0;
    }

    // Check 2: Detect clearly invalid types like alphabets-only strings
    if (temp_refrehRate.contains(QRegularExpression("^[a-zA-Z\\s]+$"))) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: RefreshRate appears to be a non-numeric string: " << temp_refrehRate;
        object->endGroup();
        return 0;
    }

    bool isInt = false;
    //convert stirng to int
    int refreshRate = temp_refrehRate.toInt(&isInt);
    //check the data is actual int or not
    if (!isInt) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: refreshRate must be an integer value, actual: " << temp_refrehRate;
        object->endGroup();
        return 0;
    }

    //Check if value is negative
    if(refreshRate<0){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: refreshRate must be positive only, actual: " << refreshRate;
        object->endGroup();
        return 0;
    }


    //check the Refreshrate is not equal to zero or less than and more than 1000
    if(refreshRate<=0 || refreshRate>1000){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : refresh rate is should not less than zero and more than 1000 "<<refreshRate;
        object->endGroup();
        return 0;
    }

    // setting the RefreshRate
    test_config.Refresh_Rate = refreshRate;

    //check the loglevel is 2 to write the debug statement into the log file
    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"RefreshRate : "<<test_config.Refresh_Rate;


    //parsing and setting the ChannelID & sampling rate
    QString channelAndRate = object->value("ChannelID_SamplingRate").toString();

    if(channelAndRate.isEmpty()){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning : ChannelID_Sampling_Rate string is empty !";
        object->endGroup();
        return 0;
    }

    // Split the string by "_" to get each "channelID:rate" pair
    QStringList pairs = channelAndRate.split("_", QString::SkipEmptyParts);

    // Check 2: Detect clearly invalid types like alphabets-only strings
    if (channelAndRate.contains(QRegularExpression("^[a-zA-Z\\s]+$"))) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: ChannelID_Sampling_Rate appears to be a non-numeric string: " << channelAndRate;
        object->endGroup();
        return 0;
    }


    // Check for valid pairs, e.g., ChannelID and rate should be numbers and separated by ":"
    for (int i = 0; i < pairs.size(); ++i) {
        QStringList parts = pairs[i].split(":");
        // Check if there is an even number of elements
        if (parts.size() != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog << "warning : Invalid entry at index" << i << ":" << pairs[i];
            return 0;
        }else {
            // Check if value is a valid integer (not float or non-numeric)
            bool isIntC = false;bool isIntS = false;
            parts[0].toInt(&isIntC);
            parts[1].toInt(&isIntS);
            if (!isIntC || !isIntS) {
                activityLog << "warning: channelID must be an integer value, actual: " << parts[0] <<": "<<parts[1];
                object->endGroup();
                return 0;
            }
        }

        bool ok1 = false, ok2 = false;
        //store the value into the local variable
        int channelId = parts[0].trimmed().toInt(&ok1);
        int sampleRate = parts[1].trimmed().toInt(&ok2);

        if (ok1 && ok2) {
            //store the channelId and sampling rate into the MAP
            test_config.ChannelID_SamplingRate.insert(channelId, sampleRate);
        } else {
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog << "warning : Invalid channel ID or sample rate at index" << i << ":" << pairs[i];
            return 0;
        }
    }
    //set the test Object to the setter method
    setTestConfiguration(test_config);

    //end of test group
    object->endGroup();

    //Used to debug here or use this to anywhere [sample]
    for (auto it = test_config.ChannelID_SamplingRate.begin(); it != test_config.ChannelID_SamplingRate.end(); ++it) {

        //check the loglevel is 2 to write the debug statement into the log file
        if(getLogLevel()>LOGLEVELINFO)
            activityLog<<"channelId : "<<it.key()<<" samplerate : "<<it.value();
    }
    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"*************************End of Test_Config_method********************************************""\n";

    return 1;
}




/**
 * @brief   This thread function is used to read and store KAFKA data into the appropriate variables.
 * @param   QSetting object, access for the file
 * @retval  '1' if successfully executed '0' if any error occured
 * @details Read the data from kafka section Header on this file
 *          And store it in respective variables
 */
int Config_Handler::Read_Kafka_Config(QSettings *object){

    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"===========================KAFKA=================================";

    // Check if "KAFKA" group exists (SECTION)
    QStringList allGroups = object->childGroups();
    if (!allGroups.contains("KAFKA")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'KAFKA' section is missing in the config file";
        return 0;
    }

    // Read KAFKA section: URL, serialization, deserialization, communication topic, analog/digital topic count.
    object->beginGroup("KAFKA");

    // Check if "KafkaURL, Serializer,NoOfAnalogTopic, NoOfDigitalTopic" key exists
    QStringList allKeys = object->childKeys();
    if (!allKeys.contains("KafkaURL")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'KafkaURL' key is missing in KAFKA section";
        return 0;
    }else if (!allKeys.contains("Serializer")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'Serializer' key is missing in KAFKA section";
        return 0;
    }else if (!allKeys.contains("NoOfAnalogTopic")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'NoOfAnalogTopic' key is missing in KAFKA section";
        return 0;
    }else if (!allKeys.contains("NoOfDigitalTopic")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'NoOfDigitalTopic' key is missing in KAFKA section";
        return 0;
    }



    //create a object for KAFKA structure to store kafka details
    KAFKA kafka;

    QHostAddress ip;
    int port;
    bool ok1 = false;

    // Regular expression for strict IPv4 format validation
    QRegularExpression ipv4Strict(R"(^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$)");
    QString temp_kafka_URL;
    //parsing the value
    temp_kafka_URL = object->value("KafkaURL").toString();

    //check the url is not empty
    if(temp_kafka_URL.isEmpty()){
        if (getLogLevel() >= LOGLEVELINFO)
            activityLog<<"warning : kafkaURL value is empty "<<temp_kafka_URL;
        object->endGroup();
        return 0;
    }

    //check URL have the correct format
    QStringList parts = temp_kafka_URL.split(":");
    if (parts.size()!= 2) {
        if (getLogLevel() >= LOGLEVELINFO)
            activityLog<<"warning : kafkaURL is not proper format 'IP:PORT' actual URL is "<<temp_kafka_URL;
        object->endGroup();
        return 0;
    }else {
        ip = QHostAddress(parts[0]);
        port = parts[1].trimmed().toInt(&ok1);
    }


    // Validate kafkaIP
    if(parts[0] == "0.0.0.0")
    {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: kafka IP cannot be 0.0.0.0";
        object->endGroup();
        return 0;
    }
    else if (!ipv4Strict.match(parts[0]).hasMatch() || QHostAddress(parts[0]).isNull()) {
        if (getLogLevel() >= LOGLEVELINFO) {
            activityLog << "warning: kafka IP is not a valid IP address "<<parts[0];
            object->endGroup();
            return 0;
        }
    }

    //check port is Integer only
    if(!ok1){
        if (getLogLevel() >= LOGLEVELINFO) {
            activityLog << "warning : In KAFKA URL- PORT is not an Integer "<<port;
            object->endGroup();
            return 0;
        }
    }

    //check port should be positive and less than 9999
    if( port <=0 || port>9999){
        if (getLogLevel() >= LOGLEVELINFO) {
            activityLog << "warning : KAFKA port is should be less than 9999 or port should not be less than zero "<<port;
            object->endGroup();
            return 0;
        }
    }

    QString port_temp = QString::number(port);
    //check : port should have 4 chars
    if(port_temp.length()<=3){
        if (getLogLevel() >= LOGLEVELINFO) {
            activityLog << "warning : KAFKA port is should be greater than 999 "<<port;
            object->endGroup();
            return 0;
        }
    }

    //setting the KafkaURL
    kafka.kafkaURL = temp_kafka_URL;

    //check the loglevel is 2 to write the debug statement into the log file
    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"KafkaURL : "<<kafka.kafkaURL;


    //parsing the serializer for output topic data
    QString ser = object->value("Serializer").toString();
    bool serial = false;

    //check if the serializer of output topic is not empty or null
    if(ser.isEmpty()){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : Kafka serializer is empty "<<ser;
        object->endGroup();
        return 0;
    }
    ser.toInt(&serial);
    //check serializer is numerical value
    if(serial){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : Kafka serializer should not be numeric value  "<<ser;
        object->endGroup();
        return 0;
    }

    //check the serializer have any special characters or numeric letters
    QRegularExpression regex("[^a-zA-Z0-9]");
    if(regex.match(ser).hasMatch()){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : Kafka serializer contains special characters  "<<ser;
        object->endGroup();
        return 0;
    }

    //check serializer is byte array or jsonObject
    if(ser!="ByteArray" && ser!="JsonObject"){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : Kafka serializer is not valid serialization  "<<ser;
        object->endGroup();
        return 0;
    }


    //setting the serializer for output topic data
    kafka.serializer = ser;

    //check the loglevel is 2 to write the debug statement into the log file
    if(getLogLevel()>1)
        activityLog<<"Serializer for output topic : "<<kafka.serializer;

    //parsing the NoOfAnalogTopic
    QString analog = object->value("NoOfAnalogTopic").toString();

    //check if the NoOfAnalogTopic of output topic is not empty or null
    if(analog.isEmpty()){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : Kafka NoOfAnalogTopic is empty "<<analog;
        object->endGroup();
        return 0;
    }

    bool noOfAna = false;
    int value = analog.toInt(&noOfAna);
    //check NoOfAnalogTopic is numerical value
    if(!noOfAna){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : Kafka NoOfAnalogTopic should be Integer value only  "<<analog;
        object->endGroup();
        return 0;
    }

    // Check if the number of analog topics is greater than or equal to zero and greater than 100
    if(value<0 || value>100){
        activityLog<<"warning : NoOfAnalogTopic is negative or greater than 100 "<<analog;
        object->endGroup();
        return 0;
    }

    //setting the NoOfAnalogTopic
    kafka.NoOfAnalogTopic = value;

    //check the loglevel is 2 to write the debug statement into the log file
    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"NoOfAnalogTopic : "<<kafka.NoOfAnalogTopic;


    //parsing the NoOfDigitalTopic
    QString digital = object->value("NoOfDigitalTopic").toString();

    //check if the NoOfAnalogTopic of output topic is not empty or null
    if(digital.isEmpty()){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : Kafka NoOfDigitalTopic is empty "<<digital;
        object->endGroup();
        return 0;
    }

    bool noOfDig = false;
    int Dvalue = digital.toInt(&noOfDig);
    //check NoOfDigitalTopic is numerical value
    if(!noOfDig){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : Kafka NoOfDigitalTopic should be Integer value only  "<<digital;
        object->endGroup();
        return 0;
    }

    // Check if the number of digital topics is greater than or equal to zero and greater than 100
    if(Dvalue<0 || Dvalue>100){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : NoOfDigitalTopic is negative or greater than 100 "<<Dvalue;
        object->endGroup();
        return 0;
    }

    //setting the NoOfDigitalTopic
    kafka.NoOfDigitalTopic = Dvalue;

    //check the loglevel is 2 to write the debug statement into the log file
    if(getLogLevel()>LOGLEVELINFO){
        activityLog<<"NoOfDigitalTopic : "<<kafka.NoOfDigitalTopic;
    }

    //set the kafka object to setter method
    setKafkadetail(kafka);


    //end of kafka group
    object->endGroup();

    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"*************************End of Kafka_config method********************************************""\n";

    return 1;
}

/**
 * @brief   This thread function is used to read and store COMMUNICATION TOPIC data into the appropriate variables.
 * @param   QSetting object, access for the file
 * @retval  '1' if successfully executed '0' if any error occured
 * @details Read the data from commuication topic section Header on this file
 *          And store it in respective variables
 */
int Config_Handler::Read_Communication_Config(QSettings *object){

    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"============================COMMUNICATION_TOPIC==============================";


    // Check if "COMMUNICATION_TOPIC" group exists (SECTION)
    QStringList allGroups = object->childGroups();
    if (!allGroups.contains("COMMUNICATION_TOPIC")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'COMMUNICATION_TOPIC' section is missing in the config file";
        return 0;
    }

    // Read COMMUNICATION_TOPIC section: Topic Name, serializer, deserializer
    object->beginGroup("COMMUNICATION_TOPIC");

    // Check if "TopicName, Serializer, DeSerializer key exists
    QStringList allKeys = object->childKeys();
    if (!allKeys.contains("Topic_Name")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'Topic_Name' key is missing in COMMUNICATION_TOPIC section";
        object->endGroup();
        return 0;
    }else if (!allKeys.contains("Serializer")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'Serializer' key is missing in COMMUNICATION_TOPIC section";
        object->endGroup();
        return 0;
    }else if (!allKeys.contains("DeSerializer")) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "warning: 'DeSerializer' key is missing in COMMUNICATION_TOPIC section";
        object->endGroup();
        return 0;
    }


    //create a object for COMMUNICATION_TOPIC structure to store the communication details
    COMMUNICATION_TOPIC comm;

    //parsing the Communication_Topic
    QString topic = object->value("Topic_Name").toString();


    QRegularExpression regex("[*&\\(\\)|?!]");  // Regex for unwanted characters

    QRegularExpressionMatch match = regex.match(topic);
    bool topicB=false;
    // Regular expression for matching only alphapetic
    topic.toInt(&topicB);
    //check serializer is numerical value
    if(topicB){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : communication topic name should not be numeric value  "<<topic;
        object->endGroup();
        return 0;
    }

    // Check if the communication topic is not empty or null
    if(topic.isEmpty() || topicB || match.hasMatch()){
        activityLog<<"warning : communication topicname is empty or not correct "<<topic;
        object->endGroup();
        return 0;
    }else {
        //setting the Communication_Topic
        comm.topic_name = topic;

        //check the loglevel is 2 to write the debug statement into the log file
        if(getLogLevel()>LOGLEVELINFO)
            activityLog<<"CommunicationTopic : "+comm.topic_name;
    }



    //parsing the Serializer
    QString ser = object->value("Serializer").toString();

    QRegularExpression regex1("[*&\\(@\\)|?!]");  // Regex for unwanted characters

    QRegularExpressionMatch match1 = regex1.match(ser);
    bool serialize=false;
    // Regular expression for matching only alphapetic
    ser.toInt(&serialize);
    //check serializer is numerical value
    if(serialize){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : communication topic serializer should not be numeric value  "<<ser;
        object->endGroup();
        return 0;
    }

    // Check if the communication topic is not empty or null
    if(ser.isEmpty() || serialize || match1.hasMatch()){
        activityLog<<"warning : communication serializer is empty or not correct "<<ser;
        object->endGroup();
        return 0;
    }else {
        // setting the Serializer
        comm.serializer = ser;

        //check the loglevel is 2 to write the debug statement into the log file
        if(getLogLevel()>LOGLEVELINFO)
            activityLog<<"Comm_DataSerializer : "+comm.serializer;
    }

    //parsing setting the DeSerializer
    QString deser = object->value("DeSerializer").toString();




    // Regular expression for matching only numeric digits
    QRegularExpression regex_Deserialize1("^\\d+$");

    QRegularExpressionMatch match_Deserialize1 = regex_Deserialize1.match(deser);

    QRegularExpression regex_Deserialize("[*&\\(@\\)|?!]");  // Regex for unwanted characters

    QRegularExpressionMatch match_Deserialize = regex_Deserialize.match(deser);
    bool Deserialize=false;
    // Regular expression for matching only alphapetic
    ser.toInt(&Deserialize);
    //check serializer is numerical value
    if(Deserialize){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : communication Deserializer should not be numeric value  "<<deser;
        object->endGroup();
        return 0;
    }

    // Check if the communication Deserializer is not empty or null
    if(deser.isEmpty() || match_Deserialize1.hasMatch() || match_Deserialize.hasMatch()){
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog<<"warning : communication Deserializer is empty or not correct "<<deser;
        object->endGroup();
        return 0;
    }
    //setting the DeSerializer
    comm.deSerializer  =deser;

    //check the loglevel is 2 to write the debug statement into the log file
    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"Comm_Deserializer : "+comm.deSerializer;

    //setting the communication object to setter method
    setCommunicationdetail(comm);

    //end communication topic group
    object->endGroup();

    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"*************************End of Communication_Topic_Config method********************************************""\n";

    return 1;

}

/**
 * @brief   This thread function is used to read and store ANALOGTOPIC data into the appropriate variables.
 * @param   QSetting object, access for the file
 * @retval  '1' if successfully executed '0' if any error occured
 * @details Read the data from analogtopic section Header on this file
 *          And store it in respective variables
 */
int Config_Handler::Read_Analog_Topic_Config(QSettings *object){

    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"==========================ANALOG_TOPIC==============================";

    // Get all the group names from the config
    QStringList allGroups = object->childGroups();
    int topicCount = getKafkadetail().NoOfAnalogTopic;
    int actualAnalogTopics = 0;

    // Count how many groups start with "Analog_Topic_"
    for (const QString &groupName : allGroups) {
        if (groupName.startsWith("Analog_Topic_")) {
            actualAnalogTopics++;
        }
    }

    // Compare actual vs expected
    if (actualAnalogTopics > topicCount) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "Warning: Config contains more Analog_Topic headers ("
                        << actualAnalogTopics << ") than expected (" << topicCount << ")";
        return 0;
    } else if (actualAnalogTopics < topicCount) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "Warning: Config contains fewer Analog_Topic headers ("
                        << actualAnalogTopics << ") than expected (" << topicCount << ")";
        return 0;
    }


    for (int i =1;i<=topicCount;i++) {

        // Read all Analog_Topic_N groups and store topicName, min/max sampling rate in map.
        object->beginGroup("Analog_Topic_"+QString::number(i));

        //created a new structure object for each cycle to store all analog topic's detail
        Analog_output_topic topic;

        // Check if "Topic_Name" key exists
        QStringList allKeys = object->childKeys();
        if (!allKeys.contains("Topic_Name")) {
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog << "warning: 'Topic_Name' key is missing in Analog_Topic "+QString::number(i);
            object->endGroup();
            return 0;
        }
        // Check if "Min" key exists
        if (!allKeys.contains("Min")) {
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog << "warning: 'Min' key is missing in Analog_Topic "+QString::number(i);
            object->endGroup();
            return 0;
        }

        // Check if "Max" key exists
        if (!allKeys.contains("Max")) {
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog << "warning: 'Max' key is missing in Analog_Topic "+QString::number(i);
            object->endGroup();
            return 0;
        }

        //parsing the Analog_Output_Topic
        QString topicA = object->value("Topic_Name").toString();

        // Define the regex pattern for "TOPIC_A_" followed by any characters
        QRegularExpression regex("^TOPIC_A_.*");

        // Perform the regex match
        QRegularExpressionMatch match = regex.match(topicA);


        // check topic name for the analog output topic is not empty or null
        if(topicA.isEmpty() || !match.hasMatch()){
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog <<"warning : topic name is should not be empty or invalid format for analog "<<topicA;
            object->endGroup();
            return 0;
        }
        //setting the Analog_Output_Topic
        topic.topicName = topicA;

        //parsing the Minimum_Sampling_Rate
        int min = object->value("Min").toInt();


        // Check if the minimum sampling rate is greater than zero
        if(min<=0 || min>9999){
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog <<"warning : min sampling rate should not be less than zero or more than 9999 for Analogtopic "<<min;
            return 0;
        }
        //settting the Minimum_Sampling_Rate
        topic.minimum_sampling_rate = min;

        //parsing the Maximum_Sampling_Rate
        int max = object->value("Max").toInt();

        // Check if the maximum sampling rate is greater than zero and less than 99999
        if(max<=0 || max>9999 || max<min){
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog <<"warning : max sampling rate should not be less than zero or more than 9999 or less than MinSampling for Analogtopic "<<max;
            return 0;
        }
        //setting the Maximum_Sampling_Rate
        topic.maximum_sampling_rate = max;

        // Check if the minimum sampling rate is greater than maximum sampling rate
        if(topic.minimum_sampling_rate > topic.maximum_sampling_rate){
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog<<"warning : min sampling rate is more than max sampling rate for topic : "<<topic.topicName;
            return 0;
        }

        //Insert the structure with number into the map
        Analog_Output_topic_detail.insert(i,topic);

        //end analog topic group
        object->endGroup();
    }
    //setting the map value
    setAnalogOutputTopicDetail(Analog_Output_topic_detail);

    //Used to debug here or use this to anywhere [sample]
    const QMap<int, Analog_output_topic> &mapRef = getAnalogOutputTopicDetail();
    for (auto it = mapRef.begin(); it != mapRef.end(); ++it) {

        //check the loglevel is 2 to write the debug statement into the log file
        if(getLogLevel()>LOGLEVELINFO)
            activityLog<<"AnalogOutputTopicDetail : "+it.value().topicName+" "+QString::number(it.value().minimum_sampling_rate)+" "+QString::number(it.value().maximum_sampling_rate);

    }
    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"*************************End of Analog_Topic_Config method********************************************""\n";

    return 1;
}

/**
 * @brief   This thread function is used to read and store DIGITALTOPIC data into the appropriate variables.
 * @param   QSetting object, access for the file
 * @retval  '1' if successfully executed '0' if any error occured
 * @details Read the data from digital topic section Header on this file
 *          And store it in respective variables
 */
int Config_Handler::Read_Digital_Topic_Config(QSettings *object){

    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"=========================DIGITAL_TOPIC==============================";

    // Get all the group names from the config
    QStringList allGroups = object->childGroups();

    int digital_topicCount = getKafkadetail().NoOfDigitalTopic;
    int actualDigitalTopics = 0;

    // Count how many groups start with "Digital_Topic_"
    for (const QString &groupName : allGroups) {
        if (groupName.startsWith("Digital_Topic_")) {
            actualDigitalTopics++;
        }
    }

    // Compare actual vs expected
    if (actualDigitalTopics > digital_topicCount) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "Warning: Config contains more Digital_Topic headers ("
                        << actualDigitalTopics << ") than expected (" << digital_topicCount << ")";
        return 0;
    } else if (actualDigitalTopics < digital_topicCount) {
        if(getLogLevel()>=LOGLEVELINFO)
            activityLog << "Warning: Config contains fewer Digital_Topic headers ("
                        << actualDigitalTopics << ") than expected (" << digital_topicCount << ")";
        return 0;
    }

    for (int i =1;i<=digital_topicCount;i++) {

        // Read Digital_Topic group and store topic name.
        object->beginGroup("Digital_Topic_"+QString::number(i));

        //created a new structure object for each cycle to store all digital topic's detail
        Digital_output_topic digital;


        // Check if "Topic_Name" key exists
        QStringList allKeys = object->childKeys();
        if (!allKeys.contains("Topic_Name")) {
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog << "warning: 'Topic_Name' key is missing in Digital_Topic "+QString::number(i);
            object->endGroup();
            return 0;
        }
        // Check if "SamplingRate" key exists
        if (!allKeys.contains("Sampling_Rate")) {
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog << "warning: 'SamplingRate' key is missing in Digital_Topic "+QString::number(i);
            object->endGroup();
            return 0;
        }

        //parsing the Digital_Output_Topic name
        QString topic = object->value("Topic_Name").toString();


        // Define the regex pattern for "TOPIC_D_" followed by any characters
        QRegularExpression regex("^TOPIC_D_.*");

        // Perform the regex match
        QRegularExpressionMatch match = regex.match(topic);


        // check topic name for the digital output topic is not empty or null
        if(topic.isEmpty() || !match.hasMatch()){
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog <<"warning : topic name is should not be empty or invalid type for digital "<<topic;
            return 0;
        }
        //setting Digital_Output_Topic name
        digital.topicName = topic;

        //parsing the Sampling_Rate
        QString sample = object->value("Sampling_Rate").toString();

        if(sample.isEmpty()){
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog<<"warning : sampling rate should not be empty for digital topic "<<digital.topicName<<" but actual sampling rate is "<<sample;
            return 0;
        }
        int sampleRate = sample.toInt();

        // Check if the minimum sampling rate is greater than zero
        if(sampleRate<0 || sampleRate>1000){
            if(getLogLevel()>=LOGLEVELINFO)
                activityLog<<"warning : sampling rate should not be more than 1000ms and less than 0 for digital topic "<<digital.topicName<<" but actual sampling rate is "<<sampleRate;
            return 0;
        }

        //setting the sampleRate
        digital.sampling_rate = sampleRate;

        //Insert the all digital topic details into the map
        Digital_Output_topic_detail.insert(i,digital);

        //end digital topic group
        object->endGroup();
    }
    //set the map into the setter method
    setDigitalOutputTopicDetail(Digital_Output_topic_detail);

    //Used to debug here or use this to anywhere [sample]
    const QMap<int, Digital_output_topic> &mapRef1 = getDigitalOutputTopicDetail();
    for (auto it = mapRef1.begin(); it != mapRef1.end(); ++it) {

        //check the loglevel is 2 to write the debug statement into the log file
        if(getLogLevel()>LOGLEVELINFO)
            activityLog<<"DigitalOutputTopicDetail : "+it.value().topicName+" "+QString::number(it.value().sampling_rate);

    }

    if(getLogLevel()>LOGLEVELINFO)
        activityLog<<"*************************End of Digital_Topic_Config method********************************************""\n";

    return 1;
}

