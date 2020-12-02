using System;
using System.Linq;
using System.Threading;
using StackExchange.Redis;

namespace RedisKeyspaceNotifications
{
    class Program
    {
        private const string StrConn =
            "localhost:6379,ConnectTimeout=5000,configCheckSeconds=30,keepAlive=20,syncTimeout=5000,defaultDatabase=3";

        private const string EXPIRED_KEYS_CHANNEL = "__keyevent@3__:expired";
        private static readonly ConnectionMultiplexer _cacheConnection;
        private static IDatabase _database;

        static Program()
        {
            _cacheConnection = ConnectionMultiplexer.Connect(StrConn);
            _database = _cacheConnection.GetDatabase();
        }

        private static void RegisterSubscribers()
        {
            ISubscriber subscriber = _cacheConnection.GetSubscriber();
            subscriber.Subscribe(EXPIRED_KEYS_CHANNEL, (channel, key) =>
            {
                Console.WriteLine($"EXPIRED -> {key}");
            });
        }

        private static void SendCache()
        {
            var rnd = new Random();
            var instances = Enumerable.Range(1, 50).ToArray();

            for (int i = 0; i < 1000; i++)
            {
                Thread.Sleep(TimeSpan.FromSeconds(2));

                var key = instances[rnd.Next(50)];
                var value = DateTime.Now.ToString("HH:mm:ss.ffff");
                _database.StringSet($"Key: {key.ToString()}", value: value, TimeSpan.FromSeconds(5));
            }
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Redis Keyspace Notifications !");
            
            RegisterSubscribers();
            
            SendCache();

            Console.WriteLine("Listening for events...");
            Console.ReadKey();
        }
    }
}