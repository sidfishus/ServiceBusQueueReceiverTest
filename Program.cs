
using System;
using Azure.Messaging.ServiceBus;
using System.Threading.Tasks;

namespace ServiceBusQueueReceiverTest
{
    static class Program
    {
        //sidtodo DO NOT COMMIT THE CONNECTION STRING!
        static string _connectionString = "..";

        static string _queueName = "TestQueue";

        static async Task Main(string[] args)
        {
            // Most of this is a copy and paste from
            // https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dotnet-get-started-with-queues.

            await using (var client = new ServiceBusClient(_connectionString))
            //await using (var processor = client.CreateProcessor(_queueName, new ServiceBusProcessorOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete }))
            await using (var processor = client.CreateProcessor(_queueName, new ServiceBusProcessorOptions{ ReceiveMode = ServiceBusReceiveMode.PeekLock }))
            {
                try
                {
                    processor.ProcessMessageAsync += MessageHandler;

                    // add handler to process any errors
                    processor.ProcessErrorAsync += ErrorHandler;

                    // start processing 
                    await processor.StartProcessingAsync();

                    Console.WriteLine("Processing messages for 1 minute before stopping.");

                    await Task.Delay(1000 * 60 /* 1 minute */);

                    // stop processing 
                    Console.WriteLine("\nStopping the receiver...");
                    await processor.StopProcessingAsync();
                    Console.WriteLine("Stopped receiving messages");
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Exception inside main thread: {e.ToString()}");
                }
            }

        }

        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body}");

            //TODO works with peek/lock mode.
            // Give the message to another queue consumer.
            await args.AbandonMessageAsync(args.Message);
            //await args.CompleteMessageAsync(args.Message);
        }

        // handle any errors when receiving messages
        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }
    }
}
