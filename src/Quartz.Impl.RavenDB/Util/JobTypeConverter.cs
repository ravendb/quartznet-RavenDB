using System;
using Newtonsoft.Json;

namespace Quartz.Impl.RavenDB.Util
{
    internal class JobTypeConverter : JsonConverter<Type>
    {
        public override bool CanRead => false;

        public override bool CanWrite => true;

        public override Type ReadJson(JsonReader reader, Type objectType, Type existingValue, bool hasExistingValue,
            JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override void WriteJson(JsonWriter writer, Type value, JsonSerializer serializer)
        {
            writer.WriteValue($"{value.FullName}, {value.Assembly.GetName().Name}");
        }
    }
}