using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

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
            var isDoneReceivedSuccessfully = new TaskCompletionSource<bool>();
            
            token.Register(async () =>
            {
                await _client.CloseAsync();
                isDoneReceivedSuccessfully.SetResult(true);
            });
            
            _client.RegisterSessionHandler(
                async (session, message, _) =>
                {
                    try
                    {
                        var stateMetadata = await session.GetStateAsync();

                        var sessionState = (IsEmptyStateMetadata(stateMetadata))
                            ? new SessionStateControl() 
                            : DeserializedSessionState(stateMetadata);

                        if (IsNextMessageInSequence(message, sessionState))
                        {
                            if (ProcessMessages(message))
                            {
                                await session.CompleteAsync(GetMessageLockToken(message));

                                sessionState.LastProcessedVersion = GetAggregateVersionByMessage(message);

                                await session.SetStateAsync(SerializedSessionState(sessionState));

                                if (IsLastMessage(message))
                                {
                                    CloseSession(session);
                                }
                            }
                            else
                            {
                                await _client.DeadLetterAsync(GetMessageLockToken(message),
                                    "The message cannot be processed", "Cannot deserialize this message");
                            }
                        }
                        else
                        {
                            sessionState.DeferredMessages.Add(GetAggregateVersionByMessage(message), GetSequenceIdByMessage(message));
                            await session.DeferAsync(GetMessageLockToken(message));
                            await session.SetStateAsync(SerializedSessionState(sessionState));
                        }

                        await ProcessNextDeferredMessages(session, sessionState);
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

            await isDoneReceivedSuccessfully.Task;
        }

        private static async Task ProcessNextDeferredMessages(IMessageSession session, SessionStateControl sessionState)
        {
            var expectedVersion = (short)(sessionState.LastProcessedVersion + 1);

            long deferredMessageSequenceId;

            while (true)
            {
                if (!TryGetDeferredMessageSequenceIdByExpectedVersion(sessionState, expectedVersion, out deferredMessageSequenceId))
                {
                    break;
                }

                var deferredMessage = await session.ReceiveDeferredMessageAsync(deferredMessageSequenceId);

                if (ProcessMessages(deferredMessage))
                {
                    await session.CompleteAsync(GetMessageLockToken(deferredMessage));

                    if (IsLastMessage(deferredMessage))
                    {
                        CloseSession(session);
                    }
                    else
                    {
                        sessionState.LastProcessedVersion = GetAggregateVersionByMessage(deferredMessage);

                        sessionState.DeferredMessages.Remove(expectedVersion);

                        await session.SetStateAsync(SerializedSessionState(sessionState));
                    }
                }
                else
                {
                    await _client.DeadLetterAsync(GetMessageLockToken(deferredMessage),
                        "The message cannot be processed", "Cannot deserialize this message");

                    sessionState.DeferredMessages.Remove(expectedVersion);

                    await session.SetStateAsync(SerializedSessionState(sessionState));
                }

                expectedVersion++;
            }
        }

        private static bool ProcessMessages(Message message)
        {
            lock (_messageLockerObject)
            {
                var messageProcessSuch = $"\nMessage received: \nMessageId = {message.MessageId}," +
                                         $" \nSequenceNumber = {message.SystemProperties.SequenceNumber}," +
                                         $" \nEnqueuedTimeUtc = {message.SystemProperties.EnqueuedTimeUtc}," +
                                         $"\nSession: {message.SessionId}," +
                                         $"\nOrder: {GetAggregateVersionByMessage(message)}";

                Console.WriteLine(messageProcessSuch);
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
            JsonSerializer.Serialize<SessionStateControl>(sessionState);
    }
}