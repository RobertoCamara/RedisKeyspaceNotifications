using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace RedisKeyspaceNotifications
{
    class Program
    {
        private const string StrConn =
            "localhost:6379,ConnectTimeout=5000,configCheckSeconds=30,keepAlive=20,syncTimeout=5000,defaultDatabase=0";

        private static Random _random = new Random();
        private static readonly ConnectionMultiplexer _cacheConnection;
        private static IDatabase _database;

        static Program()
        {
            _cacheConnection = ConnectionMultiplexer.Connect(StrConn);
            _database = _cacheConnection.GetDatabase();
        }

        private static bool GetRandomBool()
        {
            return _random.Next(2) == 1;
        }
        
        private static string GetKey(string channel)
        {
            return !channel.Contains(":") ? 
                string.Empty : 
                channel.Split(":")[1];
        }

        private static string GetKeyChannel(string channel)
        {
            if(!channel.Contains(":")) throw new FormatException("The channel has an invalid format");
            return channel.Split(":")[1];
        }

        private static string GetChannelByDatabase(int db = 0) => $"__keyspace@{db}__:*";
        
        private static void RegisterSubscribers(int db = 0)
        {
            ISubscriber subscriber = _cacheConnection.GetSubscriber();
            
            subscriber.Subscribe(GetChannelByDatabase(db), (channel, notificationType) =>
            {
                var key = GetKey(channel);
                switch (notificationType) // use "Kxge$" keyspace notification options to enable all of the below...
                {
                    case "expire": // requires the "Kg" keyspace notification options to be enabled
                        Console.WriteLine($"EXPIRATION SET FOR KEY: {key}");
                        break;
                    case "expired": // requires the "Kx" keyspace notification options to be enabled
                        Console.WriteLine($"KEY EXPIRED: {key}");
                        break;
                    case "rename_from": // requires the "Kg" keyspace notification option to be enabled
                        Console.WriteLine($"KEY RENAME(From): {key}");
                        break;
                    case "rename_to": // requires the "Kg" keyspace notification option to be enabled
                        Console.WriteLine($"KEY RENAME(To): {key}");
                        break;
                    case "del": // requires the "Kg" keyspace notification option to be enabled
                        Console.WriteLine($"KEY DELETED: {key}");
                        break;
                    case "evicted": // requires the "Ke" keyspace notification option to be enabled
                        Console.WriteLine($"KEY EVICTED: {key}");
                        break;
                    case "set": // requires the "K$" keyspace notification option to be enabled for STRING operations
                        Console.WriteLine($"KEY SET: {key}");
                        break;
                    default:
                        Console.WriteLine($"Invalid format: {notificationType}");
                        break;
                }
            });
        }

        private static void SetCache()
        {
            Task.Run(() =>
            {
                var instances = Enumerable.Range(1, 50).ToArray();

                for (int i = 0; i < 1000; i++)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(5));

                    var key = instances[_random.Next(50)];
                    var value = DateTime.Now.ToString("HH:mm:ss.ffff");
                    _database.StringSet($"{key.ToString()}", value: value, TimeSpan.FromSeconds(5));
                }
            });
        }

        private static void RenameCache()
        {
            Task.Run(() =>
            {
                while (true)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(2));
                    
                    if (!GetRandomBool()) continue;
                    
                    _database.StringSet("Roberto", DateTime.Now.ToString());
                    _database.KeyRename("Roberto", "Roberto Camara");
                }
            });
        }

        private static void DeleteCache()
        {
            Task.Run(() =>
            {
                while (true)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(2));
                    
                    if (!GetRandomBool()) continue;
                    
                    _database.KeyDelete("Roberto Camara");
                }
            });
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Redis Keyspace Notifications !");
            
            RegisterSubscribers();
            
            SetCache();
            RenameCache();
            DeleteCache();

            Console.WriteLine("Listening for events...");
            Console.ReadKey();
        }
    }
}