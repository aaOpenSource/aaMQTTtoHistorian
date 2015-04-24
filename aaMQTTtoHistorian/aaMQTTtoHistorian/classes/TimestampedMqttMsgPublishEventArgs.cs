using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using uPLibrary.Networking.M2Mqtt;

namespace aaMQTTtoHistorian
{
    class TimestampedMqttMsgPublishEventArgs
    {        
        uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e { get; set; }
        DateTime Timestamp { get; set; }

        public TimestampedMqttMsgPublishEventArgs()
        {
            // Empty Constructor
        }

        public TimestampedMqttMsgPublishEventArgs(uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e)
        {
            this.e = e;
            this.Timestamp = DateTime.Now;
        }

        public TimestampedMqttMsgPublishEventArgs(uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e, DateTime timestamp)
        {
            this.e = e;
            this.Timestamp = timestamp;
        }
    }
}
