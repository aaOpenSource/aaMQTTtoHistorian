using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ArchestrA;

namespace aaMQTTtoHistorian
{
    public class SubscriptionList
    {
        public List<subscription> subscribetags { get; set; }
        public List<topicnamereplacement> topicnamereplacements { get; set; }
    }

    public class subscription
    {
        public string topic { get; set; }
        public string regexmatch { get; set; }
        public bool useregex { get; set; }
        public byte qoslevel { get; set; }
        public HistorianTag historianTag { get; set; }
        public bool tagisready {get;set;}        
    }

    public class topicnamereplacement
    {
        public string find {get;set;}
        public string replace { get; set; }
    }
}
