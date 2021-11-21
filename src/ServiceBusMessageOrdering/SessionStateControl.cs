using System.Collections.Generic;

namespace ServiceBusMessageOrdering
{
    /// <summary>
    /// Classe que o subiscriber utiliza para controlar
    /// a ordem de processamento das mensagens que chegaram
    /// </summary>
    public class SessionStateControl
    {
        private const short DefaultPreStartVersion = 0;
        
        /// <summary>
        /// Int = AggregateVersion
        /// Long = SequenceId
        /// </summary>
        public Dictionary<short, long> DeferredMessages { get; set; }
        
        public short LastProcessedVersion { get; set; }
        
        public SessionStateControl()
        {
            LastProcessedVersion = DefaultPreStartVersion;
            DeferredMessages = new Dictionary<short, long>();
        }
    }
}