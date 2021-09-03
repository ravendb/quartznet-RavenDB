using System;
using System.Runtime.CompilerServices;
using JetBrains.Annotations;
using Newtonsoft.Json;
using Quartz.Core;
using Quartz.Spi;
using Raven.Client.Documents;

namespace Quartz.Impl.RavenDB
{
    /// <summary>
    ///     An implementation of <see cref="IJobStore" /> to use ravenDB as a persistent Job Store.
    ///     Mostly based on RAMJobStore logic with changes to support persistent storage.
    ///     Provides an <see cref="IJob" />
    ///     and <see cref="ITrigger" /> storage mechanism for the
    ///     <see cref="QuartzScheduler" />'s use.
    /// </summary>
    /// <remarks>
    ///     Storage of <see cref="IJob" /> s and <see cref="ITrigger" /> s should be keyed
    ///     on the combination of their name and group for uniqueness.
    /// </remarks>
    /// <seealso cref="QuartzScheduler" />
    /// <seealso cref="IJobStore" />
    /// <seealso cref="ITrigger" />
    /// <seealso cref="IJob" />
    /// <seealso cref="IJobDetail" />
    /// <seealso cref="JobDataMap" />
    /// <seealso cref="ICalendar" />
    /// <author>Iftah Ben Zaken</author>
    public partial class RavenJobStore : IJobStore
    {
        private static long _ftrCtr = SystemTime.UtcNow().Ticks;

        private TimeSpan _misfireThreshold = TimeSpan.FromSeconds(5);

        private ISchedulerSignaler _signaler;

        /// <summary>
        ///     Deserialized array of strings taken from <see cref="Urls"/>.
        /// </summary>
        private string[] _urls;

        /// <summary>
        ///     The database to use for this <see cref="RavenJobStore" /> instance.
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        ///     Only here to satisfy the object creation. We always attempt to (de-)serialize any value type in the Job Data Map
        ///     anyway, not just strings.
        /// </summary>
        [UsedImplicitly]
        public bool UseProperties { get; set; }

        /// <summary>
        ///     Gets the URL(s) to the database server(s).
        /// </summary>
        [UsedImplicitly]
        public string Urls
        {
            get => JsonConvert.SerializeObject(_urls);
            set => _urls = JsonConvert.DeserializeObject<string[]>(value);
        }

        /// <summary>
        ///     Gets the path to the certificate to authenticate against the database.
        /// </summary>
        public string CertPath { get; set; }

        /// <summary>
        ///     Gets the password to the certificate to authenticate against the database.
        /// </summary>
        public string CertPass { get; set; }

        /// <summary>
        ///     Gets the current configured <see cref="DocumentStore" />.
        /// </summary>
        private IDocumentStore Store { get; set; }

        [UsedImplicitly]
        protected virtual DateTimeOffset MisfireTime
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                var misfireTime = SystemTime.UtcNow();
                if (MisfireThreshold > TimeSpan.Zero)
                    misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);

                return misfireTime;
            }
        }

        /// <summary>
        ///     The time span by which a trigger must have missed its
        ///     next-fire-time, in order for it to be considered "misfired" and thus
        ///     have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public virtual TimeSpan MisfireThreshold
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get => _misfireThreshold;
            [MethodImpl(MethodImplOptions.Synchronized)]
            set
            {
                if (value.TotalMilliseconds < 1) throw new ArgumentException("MisfireThreshold must be larger than 0");
                _misfireThreshold = value;
            }
        }

        public bool SupportsPersistence => true;

        public long EstimatedTimeToReleaseAndAcquireTrigger => 100;

        public bool Clustered => false;

        public string InstanceId { get; set; } = "instance_two";

        public string InstanceName { get; set; } = "UnitTestScheduler";

        public int ThreadPoolSize { get; set; }
    }
}