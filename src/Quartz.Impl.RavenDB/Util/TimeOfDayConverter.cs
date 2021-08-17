using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Quartz.Impl.RavenDB.Util
{
    /// <summary>
    ///     Assists in deserialization of a JSON object to <see cref="TimeOfDay" />.
    /// </summary>
    internal class TimeOfDayConverter : JsonConverter<TimeOfDay>
    {
        public override bool CanWrite => false;

        public override void WriteJson(JsonWriter writer, TimeOfDay? value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override TimeOfDay? ReadJson(JsonReader reader, Type objectType, TimeOfDay? existingValue,
            bool hasExistingValue,
            JsonSerializer serializer)
        {
            var jo = JObject.Load(reader);

            var hour = int.Parse((string) jo["Hour"]);
            var minute = int.Parse((string) jo["Minute"]);
            var second = int.Parse((string) jo["Second"]);

            return new TimeOfDay(hour, minute, second);
        }
    }
}