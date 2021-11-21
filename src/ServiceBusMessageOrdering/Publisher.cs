using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;

namespace ServiceBusMessageOrdering
{
    /// <summary>
    /// Quem publica as mensagens no tópico
    /// </summary>
    public class Publisher
    {
        private const string ServiceBusConnectionString = "Endpoint=";
        private const string TopicToPublish = "xpto";
        

        private MessageSender _messageSender = new MessageSender(ServiceBusConnectionString, TopicToPublish);
        private TimeSpan _messageLifeTimeInMinutes = TimeSpan.FromMinutes(5);


        public Task Run()
        {
            List<dynamic> fakeMessagesToPublish = new List<dynamic>();

            var sessionId = Guid.NewGuid();
            
            PopulateMessageList(fakeMessagesToPublish, sessionId);

            return PublishMessagesToSession(fakeMessagesToPublish);
        }
        
        public Task PublishMessagesToSession(List<dynamic> messages)
        {
            List<Message> groupedMessages = messages.Select(x =>
            {
                var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(x)))
                {
                    ContentType = "application/json",
                    MessageId = $"{x.eventName}-Version:{x.aggregateVersion}-{Guid.NewGuid()}",
                    Label = "Session with ordering",
                    TimeToLive = _messageLifeTimeInMinutes,
                    SessionId = $"{x.sessionKey}"
                };
                
                message.UserProperties.Add("AggregateVersion", x.aggregateVersion);
                message.UserProperties.Add("EventName", x.eventName);

                return message;
            }).ToList();

            return SendMessagesAsync(groupedMessages);
        }

        private Task SendMessagesAsync(List<Message> groupedMessages)
        {
            var tasksToSend = new List<Task>();
            
            groupedMessages.ForEach(message => tasksToSend.Add(_messageSender.SendAsync(message)));
            
            return Task.WhenAll(tasksToSend.ToArray());
        }
        
        private void PopulateMessageList(List<dynamic> messages, Guid sessionId)
        {
            messages.Add(new { eventName = "PaymentCreated", aggregateVersion = 1, sessionKey = sessionId, isLastEvent = false });
            messages.Add(new { eventName = "SentByStatement", aggregateVersion = 3, sessionKey = sessionId, isLastEvent = true });
            messages.Add(new { eventName = "PaymentProcessed", aggregateVersion = 2, sessionKey = sessionId, isLastEvent = false });
        }
    }
}