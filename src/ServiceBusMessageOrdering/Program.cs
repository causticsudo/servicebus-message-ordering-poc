using System;

namespace ServiceBusMessageOrdering
{
    class Program
    {
        private static Publisher _publisher = new Publisher();
        private static Subscriber _subscriber = new Subscriber();
        
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("Publishing messages...");

                _publisher.Run().GetAwaiter().GetResult();

                Console.ReadKey();

                Console.WriteLine("Reading messages...");
                
                _subscriber.Run().GetAwaiter().GetResult();

                Console.ReadKey();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString(), ConsoleColor.Red);
            }
        }
    }
}
