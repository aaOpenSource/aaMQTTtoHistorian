using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;
using Newtonsoft.Json;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Utility;
using System.Threading;


[assembly: log4net.Config.XmlConfigurator(ConfigFile = "log.config", Watch = true)]
namespace mqttGenerationTester
{

    class Program
    {

        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static MqttClient _mqttClient;


        private static int timerSpeed =750;
        private static int itemCount = 2000;
        private static string topicRoot = "/data/value";

        static void Main(string[] args)
        {

            try
            {
                Timer PublishTimer = new Timer(PublishData, null, 0, timerSpeed);
                ConnectMQTT();

                Console.ReadLine();
            }
            catch (Exception ex)
            {
                log.Error(ex);
            }
            finally
            {
                DisconnectMQTT();
            }
        }

        private static void PublishData(object state)
        {

            string topicName;
            double baseMultiplier;
            double value;

            try
            {
                

                baseMultiplier = System.DateTime.Now.TimeOfDay.TotalSeconds/1000;

                if (_mqttClient != null)
                {
                    if (_mqttClient.IsConnected)
                    {

                        log.Info("Publishing Values");

                        for (int i = 1; i <= itemCount; i++)
                        {
                            value = i * baseMultiplier;

                            topicName = topicRoot + i.ToString("D6");

                            log.DebugFormat("Publish {0} on topic {1}", value.ToString(), topicName);

                            _mqttClient.Publish(topicName, System.Text.UTF8Encoding.UTF8.GetBytes(value.ToString()), 1, false);
                        }
                    }
                }
            }
            catch(Exception ex)
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

                log.Info("MQTT connection status is " + _mqttClient.IsConnected.ToString());

            }
            catch
            {
                throw;
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
    }
}
