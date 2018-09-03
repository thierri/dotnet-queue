using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;


namespace queueHandler
{
    class program
    {
        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            return ConnectionMultiplexer.Connect("localhost");
        });

        static void Main(string[] args)
        {
        

            Dictionary<RedisValue, RedisValue> contrato = new Dictionary<RedisValue, RedisValue>();

            contrato.Add("Contrato", "123123");
            contrato.Add("Saldo", "10000");

            RedisJobQueue test = new RedisJobQueue(lazyConnection, "rafael");
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            for (int i = 0; i < 10000; i++)
            {
                test.AddJobAsync(contrato);
            }
            stopWatch.Stop();

            TimeSpan ts = stopWatch.Elapsed;

            // Format and display the TimeSpan value.
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);
            Console.WriteLine("RunTime " + elapsedTime);

            //test.AsManager();
            //test.AsConsumer();


        }
    }
}
