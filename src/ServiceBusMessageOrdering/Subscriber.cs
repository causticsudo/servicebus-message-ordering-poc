using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;

namespace ServiceBusMessageOrdering
{
    /// <summary>
    /// Quem se inscreve no tópico para escutar as mensagens
    /// </summary>
    public class Subscriber
    {
        private const string ServiceBusConnectionString = "xpto";
        private const string TopicName = "xpto";
        private const string SubscriptionName = "sub-xpto";
        private const short MaxNumberOfActiveSessions = 1;

        
        private static readonly object _messageLockerObject = new object();
        private static SubscriptionClient _client = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);
        private static MessageSender _messageSender = new MessageSender(ServiceBusConnectionString, TopicName);
        
        private static DateTimeOffset SchedulerTimeInUtc = DateTimeOffset.UtcNow;
        private TimeSpan _messageWaitTimeoutInSeconds = TimeSpan.FromSeconds(5);
        private TimeSpan _taskDelayInMinutes = TimeSpan.FromMinutes(5);

        public Task Run()
        {
            return ListenToTopicSubscriptionAsync();
        }

        private async Task ListenToTopicSubscriptionAsync()
        {
            var cancellationToken = new CancellationTokenSource();

            var listeners = Task.WhenAll(GetListenersAsync(cancellationToken.Token));

            await Task.WhenAll(
                Task.WhenAny(
                    Task.Run(() => Console.ReadKey()),
                    Task.Delay(_taskDelayInMinutes)
            ).ContinueWith((_) => cancellationToken.Cancel()), listeners);
        }

        private async Task GetListenersAsync(CancellationToken token)
        {
            var isReceivedSuccessfully = new TaskCompletionSource<bool>();
            
            token.Register(async () =>
            {
                await _client.CloseAsync();
                isReceivedSuccessfully.SetResult(true);
            });
            
            _client.RegisterSessionHandler(
                async (session, message, _) =>
                {
                    try
                    {
                        //Connect to session
                        var stateMetadata = await session.GetStateAsync();

                        //Exists metadata ?
                        var sessionState = (IsEmptyStateMetadata(stateMetadata))
                            ? new SessionStateControl() 
                            : DeserializedSessionState(stateMetadata);

                        //Get first queue message and check if it's the next message in the sequence
                        if (IsNextMessageInSequence(message, sessionState))
                        {
                            //Process message
                            if (ProcessMessages(message))
                            {
                                await session.CompleteAsync(GetMessageLockToken(message));

                                //Save the version with the last one successfully processed
                                sessionState.LastProcessedVersion = GetAggregateVersionByMessage(message);

                                //Update the session state
                                await session.SetStateAsync(SerializedSessionState(sessionState));

                                //Commit message
                                //TODO: implement logic
                                
                                //Process the deferred messages
                                await ProcessNextDeferredMessages(session, sessionState);
                                
                                //If is the last message
                                if (IsLastMessage(message))
                                {
                                    //Clean and Close the session
                                    CloseSession(session);
                                }
                            }
                            else
                            {
                                //Return message to queue
                                await _messageSender.SendAsync(message);
                            }
                        }
                        else
                        {
                            //If it's the message with the wrong version
                            //Put message in the deferred list
                            sessionState.DeferredMessages.Add(GetAggregateVersionByMessage(message),
                                GetSequenceIdByMessage(message));
                            
                            //Defer message
                            await session.DeferAsync(GetMessageLockToken(message));
                            
                            //Update the session state
                            await session.SetStateAsync(SerializedSessionState(sessionState));
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Failed to receive the message: {message.MessageId} \nException: {e}");
                    }
                },
                new SessionHandlerOptions(x => LogMessageHandlerException(x))
                {
                    MessageWaitTimeout = _messageWaitTimeoutInSeconds,
                    MaxConcurrentSessions = MaxNumberOfActiveSessions,
                    AutoComplete = false
                });

            await isReceivedSuccessfully.Task;
        }

        private static async Task ProcessNextDeferredMessages(IMessageSession session, SessionStateControl sessionState)
        {
            var expectedVersion = CalculateTheNextExpectedVersion(sessionState);

            long deferredMessageSequenceId;

            while (true)
            {
                //Search the list of deferred messages to next with the version expected
                if (!TryGetDeferredMessageSequenceIdByExpectedVersion(sessionState, expectedVersion, out deferredMessageSequenceId))
                {
                    //if not there is a message with the expected version, return
                    break;
                }

                //Redeem the message via sequence id
                var deferredMessage = await session.ReceiveDeferredMessageAsync(deferredMessageSequenceId);

                //Process message based on your sequenceId
                if (ProcessMessages(deferredMessage))
                {
                    await session.CompleteAsync(GetMessageLockToken(deferredMessage));

                    //Save the version with the last one successfully processed
                    sessionState.LastProcessedVersion = GetAggregateVersionByMessage(deferredMessage);

                    //Remove the message from the message list deferred
                    sessionState.DeferredMessages.Remove(expectedVersion);

                    //Update the session state
                    await session.SetStateAsync(SerializedSessionState(sessionState));
                }
                else
                {
                    //schedule the message to be processed after `x` minutes
                    await _messageSender.ScheduleMessageAsync(deferredMessage, SchedulerTimeInUtc);
                }

                expectedVersion++;
            }
        }

        private static bool ProcessMessages(Message message)
        {
            lock (_messageLockerObject)
            {
                var suchMessageToProcess = $"\nMessage received: \nMessageId = {message.MessageId}," +
                                         $" \nSequenceNumber = {message.SystemProperties.SequenceNumber}," +
                                         $" \nEnqueuedTimeUtc = {message.SystemProperties.EnqueuedTimeUtc}," +
                                         $"\nSession: {message.SessionId}," +
                                         $"\nOrder: {GetAggregateVersionByMessage(message)}";

                Console.WriteLine(suchMessageToProcess);
            }

            return true;
        }

        private static async void CloseSession(IMessageSession session)
        {
            await session.SetStateAsync(null);
            await session.CloseAsync();
        }

        private static bool TryGetDeferredMessageSequenceIdByExpectedVersion(SessionStateControl sessionState, short expectedVersion, out long deferredMessageSequenceId)
        {
            return sessionState.DeferredMessages.TryGetValue(expectedVersion, out deferredMessageSequenceId);
        }
        
        private static short CalculateTheNextExpectedVersion(SessionStateControl session)
            => (short)(session.LastProcessedVersion + 1);
        
        private bool IsEmptyStateMetadata(byte[] stateMetadata) =>
            stateMetadata is null;

        private bool IsNextMessageInSequence(Message message, SessionStateControl sessionState) =>
            GetAggregateVersionByMessage(message) == (sessionState.LastProcessedVersion + 1);

        private static short GetAggregateVersionByMessage(Message message) =>
            (short)message.UserProperties["AggregateVersion"];
        
        private long GetSequenceIdByMessage(Message message) =>
            message.SystemProperties.SequenceNumber;

        private static string GetMessageLockToken(Message message) =>
            message.SystemProperties.LockToken;

        private static bool IsLastMessage(Message message) =>
            message.UserProperties["isLasEvent"].ToString()?.ToLower() == "true";

        private Task LogMessageHandlerException(ExceptionReceivedEventArgs e)
        {
            Console.Write($"Exception: \"{e.Exception.Message}\" {e.ExceptionReceivedContext.EntityPath}", ConsoleColor.DarkRed);
            return Task.CompletedTask;
        }
        
        private SessionStateControl DeserializedSessionState(byte[] stateMetadata) =>
            JsonSerializer.Deserialize<SessionStateControl>(stateMetadata);
        
        private static byte[] SerializedSessionState(SessionStateControl sessionState) =>
            JsonSerializer.Serialize<>(sessionState);
    }
}