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
using System.Text.RegularExpressions;


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

        ////timer for sending historian data values
        //private static Timer _historianQueueTimer;

        //private static int _totalTime;
        //private static int _timeCount;


        private static System.Diagnostics.Stopwatch sw;


        static void Main(string[] args)
        {
            try
            {
                log.Info("Starting " + System.AppDomain.CurrentDomain.FriendlyName);

                _HistorianTags = new SubscriptionList();
                _HistorianTags = JsonConvert.DeserializeObject<SubscriptionList>(System.IO.File.ReadAllText("subscriptions.json"));

                _historian = new HistorianAccess();
                _messageQueue = new Queue<TimestampedMqttMsgPublishEventArgs>();

                //_historianQueueTimer = new Timer(HistorianQueueTimerTick, null,0,1000);

                sw = new System.Diagnostics.Stopwatch();
                //_timeCount = 0;
                //timings = new List<float>();
              
                ConnectHistorian(_historian);
                AddHistorianTags();
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

        private static void HistorianQueueTimerTick(object state)
        {
            throw new NotImplementedException();
        }

        private static void AddHistorianTags()
        {
            HistorianAccessError error;

            try
            {
                log.Info("Adding defined historian tags");

                // Add all of the tags.  Separate logic will check to make sure the tag is ready before writing
                foreach (subscription localSubscription in _HistorianTags.subscribetags.Where(s => s.useregex.Equals(false)))
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
            }
            catch(Exception ex)
            {
                log.Error(ex);
            }

        }

        static bool TagIsReady(string tagName)
        {
            bool returnValue = false;

            try
            {
                // Check to see if we already konw the tag is ready
                returnValue = _HistorianTags.subscribetags.Single(s => s.historianTag.TagName.Equals(tagName)).tagisready;

                //We have not registered that the tag is ready so go check again
                if (!returnValue)
                {
                    HistorianTagStatus tagStatus = new HistorianTagStatus();
                    tagStatus.TagName = tagName;

                    _historian.GetTagStatusByName(ref tagStatus);

                    returnValue = !(tagStatus.Pending | tagStatus.ErrorOccurred);

                    //If it comes back ready then stick that back into the object for cacheing
                    if(returnValue)
                    {
                        _HistorianTags.subscribetags.Single(s => s.historianTag.TagName.Equals(tagName)).tagisready = returnValue;
                    }
                }
            }
            catch(Exception ex)
            {
                log.Error(ex);
                returnValue = false;
            }

            return returnValue;

        }

        static void ConnectHistorian(HistorianAccess historian)
        {
           
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

            if (historian != null)
            {
                if (!historian.CloseConnection(out error))
                {
                    Console.WriteLine("Failed to close historian connection: {0} {1} {2}",
                        error.ErrorType, error.ErrorCode, error.ErrorDescription);
                }
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
                //sw.Restart();
               
                SendDataToHistorian(e);

                //sw.Stop();

                //log.Info(sw.ElapsedTicks.ToString());

            }
            catch (Exception ex)
            {
                log.Error(ex);
            }
        }

        private static void SendDataToHistorian(uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e, bool dataFromQueue = false)
        {
            HistorianAccessError error;
            Double localValue;
            Boolean result;
            UInt32 tagKey = 0;

            try
            {

                if(!HistorianOK())
                {
                    // Put the message in the queue
                    _messageQueue.Enqueue(new TimestampedMqttMsgPublishEventArgs(e));
                    return;
                }

                // First look for an exact match, no regex
                try
                {
                    tagKey = _HistorianTags.subscribetags.Single(t => t.topic.Equals(e.Topic) && t.useregex == false).historianTag.TagKey;
                }
                catch
                {
                    // Couldn't find it or some other isse so set to 0 so the next codeset can try                
                    tagKey = 0;
                }


                if (tagKey == 0)
                {
                    tagKey = GetHistorianTagKey(e.Topic);
                }

                // If we still have tag key of 0 then just queue it and proces later
                if(tagKey == 0)
                {
                    // Put the message in the queue
                    _messageQueue.Enqueue(new TimestampedMqttMsgPublishEventArgs(e));
                    return;
                }

                // Finally check to make sure the tag is ready.  If it is not then just queue the message
                if (!TagIsReady(_HistorianTags.subscribetags.FirstOrDefault(t => t.historianTag.TagKey.Equals(tagKey)).historianTag.TagName))
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
                try
                {
                    // Get the server tag from the in memory object                
                    settingsServerTag = _HistorianTags.subscribetags.First(x => x.topic.Equals(topic)).historianTag;
                }
                catch
                {
                    // explicity set to null b/c we didn't find it
                    settingsServerTag = null;
                }
                                
                // If we can't locate then try to process via regex
                if(settingsServerTag == null)
                {
                    // Get all entries where useregex is True
                    foreach (subscription localSubscription in _HistorianTags.subscribetags.Where(u => u.useregex).ToList())
                    {
                        //Attempt to match the tagName using the regex
                        Regex testReg = new Regex(localSubscription.regexmatch);
                        if (testReg.IsMatch(topic))
                        {
                            string newTagName = testReg.Replace(topic, localSubscription.historianTag.TagName);

                            // Iterate through the topic name replacements to make sure we don't pass through any bad characters
                            foreach (topicnamereplacement tnr in _HistorianTags.topicnamereplacements)
                            {
                                newTagName = newTagName.Replace(tnr.find, tnr.replace);
                            }

                            // Push this tag back into the tags list so we can cache this information for future processing
                            subscription newSubscription = new subscription();

                            // First copy the existing local subscription
                            newSubscription = ObjectExtension.CopyObject<subscription>(localSubscription);

                            // Replace key values with this specific tag's information
                            newSubscription.topic = topic;
                            newSubscription.historianTag.TagName = newTagName;
                            newSubscription.regexmatch = "";
                            newSubscription.useregex = false;

                            //Add the subscription back into the in-memory object
                            _HistorianTags.subscribetags.Add(newSubscription);

                            //And set the local settingsServerTag to the tag we just constructed so it can be used later in the process
                            settingsServerTag = newSubscription.historianTag;

                            // Add to the in-memory list so we don't have to repeat this
                            //_HistorianTags.subscribetags.Add(newSubscription);

                            // Now get the tag key back
                            //tagKey = GetHistorianTagKey(topic);

                            // exit the foreach
                            break;
                        }
                    }
                }


                // If we already have a key then use that
                if (settingsServerTag.TagKey > 0)
                {
                    return settingsServerTag.TagKey;
                }

                // If we don't have a key then check to see if the tag already exists on the historian                 
                result = _historian.GetTagInfoByName(settingsServerTag.TagName, true, out serverTag, out error);

                if(!result)
                {
                   // Couldn't find tag so just keep going
                }
                
                // If the tag already exists, stick it back into the in memory object
                if(serverTag.TagKey > 0)
                {
                    try
                    {
                        // Set the data back into the in memory object
                        _HistorianTags.subscribetags.First(x => x.topic.Equals(topic)).historianTag = serverTag;
                    }
                    catch(Exception ex)
                    {
                        log.Error(ex);
                    }
                }
                else
                {
                    // The tag didn't exists so load it up with the configuration data we specified externally
                    serverTag = settingsServerTag;
                }

                // Now "Add" to the historian which really just gives us a handle back
                log.DebugFormat("Adding {0} to historian.", serverTag.TagName);
                result = _historian.AddTag(serverTag, out tagKey, out error);

                // The actual status of the tag addition will be checked later when the application attempts to write a tag

                if (!result)
                {
                    throw new Exception(error.ErrorDescription);
                }

                try
                {
                    // Finally set the data on the in-memory object with the tag key                
                    _HistorianTags.subscribetags.First(x => x.topic.Equals(topic)).historianTag.TagKey = tagKey;
                }
                catch(Exception ex)
                {
                    log.Error(ex);
                }

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
