using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Newtonsoft.Json;

namespace aaMQTTtoHistorian
{
    public static class ObjectExtension
    {
        /// <summary>
        /// A generic method to allow for copying an object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objSource"></param>
        /// <returns></returns>
        public static T CopyObject<T>(this object objSource)
        {

            string json = JsonConvert.SerializeObject(objSource);
            return JsonConvert.DeserializeObject<T>(json);

                //Serialize(objSource,ref stream);
                //return (T)Deserialize<T>(stream);
                ////BinaryFormatter formatter = new BinaryFormatter();
                ////formatter.Serialize(stream, objSource);
                ////stream.Position = 0;
                //return (T)formatter.Deserialize(stream);

        }

        public static void Serialize(object value, ref Stream s)
        {
            StreamWriter writer = new StreamWriter(s);
            JsonTextWriter jsonWriter = new JsonTextWriter(writer);
            JsonSerializer ser = new JsonSerializer();
            ser.Serialize(jsonWriter, value);
            jsonWriter.Flush();
        }

        public static T Deserialize<T>(Stream s)
        {
            StreamReader reader = new StreamReader(s);
            JsonTextReader jsonReader = new JsonTextReader(reader);
            JsonSerializer ser = new JsonSerializer();
            return ser.Deserialize<T>(jsonReader);
        }
    }
}
