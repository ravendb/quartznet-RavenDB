using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Quartz.Impl.RavenDB.Util
{
    internal class TimeOfDayConverter : JsonConverter
    {
        public override bool CanWrite => false;

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(TimeOfDay);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
            JsonSerializer serializer)
        {
            var jo = JObject.Load(reader);

            var hour = int.Parse((string) jo["Hour"]);
            var minute = int.Parse((string) jo["Minute"]);
            var second = int.Parse((string) jo["Second"]);

            return new TimeOfDay(hour, minute, second);
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
}