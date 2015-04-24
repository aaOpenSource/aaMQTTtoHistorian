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
    }

    public class subscription
    {
        public string topic { get; set; }        
        public byte qoslevel { get; set; }        
        public HistorianTag historianTag { get; set; } 
    }
}
