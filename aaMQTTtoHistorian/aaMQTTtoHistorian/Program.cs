using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Utility;
using ArchestrA.MxAccess;
using Newtonsoft.Json;
using ArchestrA;
using System.Threading;


[assembly: log4net.Config.XmlConfigurator(ConfigFile = "log.config", Watch = true)]
namespace aaMQTTtoHistorian
{
    class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static MqttClient _mqttClient;

        //object for MXAccess Settings
        private static SubscriptionList _HistorianTags;

        // global historian object
        private static HistorianAccess _historian;

        //queued messages with timestamps
        private static Queue<TimestampedMqttMsgPublishEventArgs> _messageQueue;

        static void Main(string[] args)
        {
            try
            {
                log.Info("Starting " + System.AppDomain.CurrentDomain.FriendlyName);

                _HistorianTags = new SubscriptionList();
                _historian = new HistorianAccess();
                _messageQueue = new Queue<TimestampedMqttMsgPublishEventArgs>();
              
                ConnectHistorian(_historian);
                SetupHistorianTags();
                ConnectMQTT();  

                Console.ReadLine();

            }
            catch (Exception ex)
            {
                log.Error(ex);
                Console.ReadLine();
            }
            finally
            {
                // Always disconnect on shutdown
                DisconnectMQTT();
                DisconnectHistorian(_historian);
            }
        }

        private static void SetupHistorianTags()
        {
            HistorianAccessError error;

            try
            {
                log.Info("Setting up historian tags");

                _HistorianTags = JsonConvert.DeserializeObject<SubscriptionList>(System.IO.File.ReadAllText("subscriptions.json"));

                // First add the tags all together.
                // It shoudl be dramatically faster to add all tags then 
                foreach (subscription localSubscription in _HistorianTags.subscribetags)
                {
                    log.DebugFormat("Adding tag {0}", localSubscription.historianTag.TagName);
                    UInt32 tagKey;                    
                    if (!_historian.AddTag(localSubscription.historianTag, out tagKey, out error))
                    {
                        log.WarnFormat("Failed to request tag creation: {0}", error.ErrorDescription);
                    }
                    else
                    {
                        // Save off the tag key we get back
                        localSubscription.historianTag.TagKey = tagKey;
                    }
                }

                //Now start checking the status for each tag
                foreach (subscription localSubscription in _HistorianTags.subscribetags)
                {
                    log.DebugFormat("Checking tag status of tag key {0}...", localSubscription.historianTag.TagKey);

                    HistorianTagStatus tagStatus = new HistorianTagStatus();
                    tagStatus.TagName = localSubscription.historianTag.TagName;

                    for (; ; )
                    {
                        _historian.GetTagStatusByName(ref tagStatus);
                        if (tagStatus.Pending)
                        {
                            log.DebugFormat("Tag status is not determined yet");
                            Thread.Sleep(1000);
                            continue;
                        }
                        if (tagStatus.ErrorOccurred)
                            log.DebugFormat("Tag creation error: {0}", tagStatus.Error.ErrorDescription);
                        else
                            log.DebugFormat("Tag {0} added successfully", localSubscription.historianTag.TagName);
                        break;
                    }

                    log.DebugFormat("Getting tag metadata from historian server for " + localSubscription.historianTag.TagName);

                    HistorianTag serverTag;
                    if (!_historian.GetTagInfoByName(localSubscription.historianTag.TagName, true, out serverTag, out error))
                        Console.WriteLine("Server tag query error: {0}", error.ErrorDescription);
                    else
                    {
                        log.DebugFormat("Tag Name           = {0}", serverTag.TagName);
                        log.DebugFormat("Tag Description    = {0}", serverTag.TagDescription);
                        log.DebugFormat("Tag Data Type      = {0}", serverTag.TagDataType);
                        log.DebugFormat("Tag Storage Type   = {0}", serverTag.TagStorageType);
                        log.DebugFormat("Tag Channel Status = {0}", serverTag.TagChannelStatus);

                        localSubscription.historianTag = serverTag;
                    }
                }
            }
            catch(Exception ex)
            {
                log.Error(ex);
            }

        }

        static void ConnectHistorian(HistorianAccess historian)
        {
            //HistorianConnectionArgs connectionArgs = new HistorianConnectionArgs();
            // provide historian network name and user credentials
            //connectionArgs.ServerName = "HistorianComputer";
            //connectionArgs.UserName = "MyDomain\\MyUserName";
            //connectionArgs.Password = "MyPassword";
            //// we will be creating tags and sending data
            //connectionArgs.ReadOnly = false;
            //// collect data locally in folder C:\\StoreForward if historian is offline
            ////connectionArgs.StoreForwardPath = "C:\\StoreForward";
            //// collect data locally until 1GB of free disk space left on drive C
            ////connectionArgs.StoreForwardFreeDiskSpace = 1024;

            HistorianConnectionArgs connectionArgs = JsonConvert.DeserializeObject<HistorianConnectionArgs>(System.IO.File.ReadAllText("historianconnectionsettings.json"));

            Console.WriteLine("Initiating asynchronous historian connection, please wait...");
            HistorianAccessError error;
            if (!historian.OpenConnection(connectionArgs, out error))
            {
                
                Console.WriteLine("Failed to initiate historian connection: {0} {1} {2}",
                    error.ErrorType, error.ErrorCode, error.ErrorDescription);
            }

            log.Info("Checking connection status...");
            HistorianConnectionStatus connectionStatus = new HistorianConnectionStatus();
            for (; ; )
            {
                historian.GetConnectionStatus(ref connectionStatus);
                if (connectionStatus.Pending)
                {
                    Console.WriteLine("Connection is pending");
                    Thread.Sleep(1000);
                    continue;
                }
                if (connectionStatus.ErrorOccurred)
                {
                    Console.WriteLine("Connection error: {0}",
                        connectionStatus.Error.ErrorDescription);
                }

                Console.WriteLine("Server = {0}, ServerStorage = {1}, StoreForward = {2}",
                    connectionStatus.ConnectedToServer,
                    connectionStatus.ConnectedToServerStorage,
                    connectionStatus.ConnectedToStoreForward);

                //Console.WriteLine("Press 'x' to continue or any other key to recheck the status");
                //if (Console.ReadKey(true).KeyChar == 'x')
                    break;
            }
        }

        static void DisconnectHistorian(HistorianAccess historian)
        {
            Console.WriteLine("Closing connection, please wait...");
            HistorianAccessError error;
            if (!historian.CloseConnection(out error))
            {
                Console.WriteLine("Failed to close historian connection: {0} {1} {2}",
                    error.ErrorType, error.ErrorCode, error.ErrorDescription);
            }
        }

        private static void DisconnectMQTT()
        {
            try
            {
                log.Info("Disconnecting MQTT Client");
                if (_mqttClient != null)
                {
                    _mqttClient.Disconnect();
                }
            }
            catch (Exception ex)
            {
                log.Error(ex);
            }
        }

        static void ConnectMQTT()
        {
            try
            {

                MQTTSettings mqttSettings = JsonConvert.DeserializeObject<MQTTSettings>(System.IO.File.ReadAllText("mqttsettings.json"));

                log.Info("Connecting to MQTT " + mqttSettings.host + ":" + mqttSettings.port.ToString());
                _mqttClient = new MqttClient(mqttSettings.host, mqttSettings.port, false, null);

                if (mqttSettings.clientid == "")
                {
                    // Generate new client id if it is blank in the mqttSettings file
                    mqttSettings.clientid = Environment.MachineName + System.Guid.NewGuid().ToString();

                    log.Info("Generated new client id " + mqttSettings.clientid);

                    // Write the mqttSettings back after generating a new GUID
                    System.IO.File.WriteAllText("mqttsettings.json", JsonConvert.SerializeObject(mqttSettings));
                }

                // Make the connection by logging in and specifying client id
                log.Info("Logging in with client ID " + mqttSettings.clientid + " and username " + mqttSettings.username);

                if (mqttSettings.username != "")
                {
                    _mqttClient.Connect(mqttSettings.clientid, mqttSettings.username, mqttSettings.password);
                }
                else
                {
                    _mqttClient.Connect(mqttSettings.clientid);
                }

                _mqttClient.MqttMsgPublishReceived += _mqttClient_MqttMsgPublishReceived;
                _mqttClient.MqttMsgSubscribed += _mqttClient_MqttMsgSubscribed;
                

                log.Info("MQTT connection status is " + _mqttClient.IsConnected.ToString());

                SubscribeMQTT();

            }
            catch
            {
                throw;
            }
        }

        private static void _mqttClient_MqttMsgSubscribed(object sender, uPLibrary.Networking.M2Mqtt.Messages.MqttMsgSubscribedEventArgs e)
        {
            log.Info(e.MessageId.ToString());
        }

        static void SubscribeMQTT()
        {
            try
            {
                log.Info("Setting up MQTT subscriptions");

                // Loop through all of the tags we are subscribing to and get those on supervisory advise so we can perform writes
                if (MQTTOK())
                {

                    //// Read in the MX Access Settings from the configuration file
                    //_HistorianTags = JsonConvert.DeserializeObject<SubscriptionList>(System.IO.File.ReadAllText("historian.json"));

                    // Now add the subscriptions
                    List<string> topics = new List<string>();
                    List<byte> qoslevels = new List<byte>();

                    // First get on advise
                    foreach (subscription sub in _HistorianTags.subscribetags)
                    {
                        log.Info("Adding Subscribe for " + sub.topic);
                            topics.Add(sub.topic);
                            qoslevels.Add(sub.qoslevel);
                        //}
                    }

                    // Now add all the subscritpions and QOS in one statement
                    _mqttClient.Subscribe(topics.ToArray<string>(), qoslevels.ToArray<byte>());
                }
            }
            catch(Exception ex)
            {
                log.Error(ex);
            }
        }

        private static void _mqttClient_MqttMsgPublishReceived(object sender, uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e)
        {
            try
            {
                //log.Info(e.Topic + " " + System.Text.ASCIIEncoding.UTF8.GetString(e.Message));
                SendDataToHistorian(e);
            }
            catch (Exception ex)
            {
                log.Error(ex);
            }
        }

        private static void SendDataToHistorian(uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e )
        {
            HistorianAccessError error;
            Double localValue;
            Boolean result;

            try
            {

                if(!HistorianOK())
                {
                    // Put the message in the queue
                    _messageQueue.Enqueue(new TimestampedMqttMsgPublishEventArgs(e));
                    return;
                }

                UInt32 tagKey = _HistorianTags.subscribetags.Find(t => t.topic.Equals(e.Topic)).historianTag.TagKey;

                if(tagKey == 0)
                {
                    // Put the message in the queue
                    _messageQueue.Enqueue(new TimestampedMqttMsgPublishEventArgs(e));
                    return;
                }

                string messageValue = System.Text.ASCIIEncoding.UTF8.GetString(e.Message);

                HistorianDataValue myVTQ = new HistorianDataValue();
                myVTQ.TagKey = tagKey;
                myVTQ.DataValueType =  _HistorianTags.subscribetags.Find(x => x.topic.Equals(e.Topic)).historianTag.TagDataType;
                myVTQ.OpcQuality = 192;

                if(Double.TryParse(messageValue,out localValue))
                {
                    myVTQ.Value = localValue;
                }
                else
                {
                    throw new Exception("Error Parsing Message to Double " + messageValue);                    
                }

                myVTQ.StartDateTime = DateTime.Now;

                result = (_historian.AddStreamedValue(myVTQ, out error));
                
                if(!result)
                {
                    throw new Exception("Failed to write VTQ : " + error.ErrorDescription);
                }

            }
            catch(Exception ex)
            {
                log.Error(ex);
            }

        }

        private static bool MQTTOK()
        {
            bool returnValue = false;

            try
            {
                if (_mqttClient != null)
                {
                    returnValue = _mqttClient.IsConnected;
                }
            }
            catch
            {
                returnValue = false;
            }

            return returnValue;

        }

        private static bool HistorianOK()
        {

            Boolean result = false;
            HistorianConnectionStatus localStatus = new HistorianConnectionStatus();

            try
            {
                _historian.GetConnectionStatus(ref localStatus);

                result = !localStatus.ErrorOccurred;
            }
            catch(Exception ex)
            {
                log.Error(ex);
                result = false;
            }

            return result;
        }

        private static UInt32 GetHistorianTagKey(string topic)
        {
            UInt32 returnValue;
            Boolean result;
            HistorianAccessError error;
            HistorianTag settingsServerTag;
            HistorianTag serverTag;
            UInt32 tagKey;

            returnValue = 0;
            
            try
            {
                // Get the server tag from the in memory object
                //settingsServerTag = _MXAccessSettings.subscribetags.Find(x => x.hitem.Equals(hitem)).historianTag;
                settingsServerTag = _HistorianTags.subscribetags.Find(x => x.topic.Equals(topic)).historianTag;

                // If we already have a key then use that
                if (settingsServerTag.TagKey > 0)
                {
                    return settingsServerTag.TagKey;
                }

                // If we don't have a key then check to see if the tag already exists on the historian                 
                result = _historian.GetTagInfoByName(settingsServerTag.TagName, true, out serverTag, out error);

                if(!result)
                {
                    if (error.ErrorCode != HistorianAccessError.ErrorValue.FailedToGetFromServer)
                    {
                        throw new Exception(error.ErrorDescription);
                    }
                }
                
                // If the tag already exists, stick it back into the in memory object
                if(serverTag.TagKey > 0)
                {
                    // Set the data back into the in memory object
                    _HistorianTags.subscribetags.Find(x => x.topic.Equals(topic)).historianTag = serverTag;
                }
                else
                {
                    // The tag didn't exists so load it up with the configuration data we specified externally
                    serverTag = settingsServerTag;
                }

                // Now "Add" to the historian which really just gives us a handle back
                result = _historian.AddTag(serverTag, out tagKey, out error);

                if (!result)
                {
                    throw new Exception(error.ErrorDescription);
                }

                // Finally set the data on the in-memory object with the tag key                
                _HistorianTags.subscribetags.Find(x => x.topic.Equals(topic)).historianTag.TagKey = tagKey;

                //And set the return value
                returnValue = tagKey;

            }
            catch(Exception ex)
            {
                log.Error(ex);
                returnValue = 0;
            }

            return returnValue;
        }
    }
}
