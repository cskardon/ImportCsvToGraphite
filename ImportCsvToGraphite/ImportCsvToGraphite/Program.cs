namespace ImportCsvToGraphite
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Net.Sockets;

    internal class Program
    {
        private static string _hostname = "localhost";
        private static string _directory = "c:\\temp\\metrics";
        private static int _port = 2003;
        private static int _rebaseSeconds;
        private static string _prefix = "Neo4j";
        private static int _incrementRebaseSeconds = 5;

        private static bool AreParametersOk(string[] args)
        {
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i].ToLowerInvariant())
                {
                    case "-hostname":
                    case "-host":
                        _hostname = args[++i];
                        break;
                    case "-port":
                        if (int.TryParse(args[++i], out int port))
                            _port = port;
                        else throw new ArgumentOutOfRangeException("port", args[i], "Port isn't a number!");
                        break;
                    case "-directory":
                    case "-d":
                        _directory = args[++i];
                        break;
                    case "-rebaseseconds":
                    case "-rs":
                        if (int.TryParse(args[++i], out int rebaseSeconds)) _rebaseSeconds = rebaseSeconds;
                        else throw new ArgumentOutOfRangeException("rebaseSeconds", args[i], "Seconds provided for RebaseSeconds wasn't a number");
                        break;
                    case "-rebasesecondsincrement":
                    case "-rsi":
                        if (int.TryParse(args[++i], out int rebaseIncrement)) _incrementRebaseSeconds = rebaseIncrement;
                        else throw new ArgumentOutOfRangeException("rebaseIncrementSeconds", args[i], "Seconds provided to increment RebaseSeconds wasn't a number.");
                        break;
                    case "-prefix":
                        _prefix = args[++i];
                        break;
                    default:
                        Console.WriteLine($"Unknown parameter supplied: '{args[i]}'");
                        ShowUsageMessage();
                        return false;
                }
            }

            if (string.IsNullOrWhiteSpace(_hostname) || string.IsNullOrWhiteSpace(_prefix) || string.IsNullOrWhiteSpace(_directory))
                return false;

            return true;
        }

        private static void Main(string[] args)
        {
            if (!AreParametersOk(args))
            {
                ShowUsageMessage();
                return;
            }

            var client = new TcpClient();
            client.Connect(_hostname, _port);
            Console.WriteLine($"Connected to the Graphite end point ({client.Connected}).");

            var stream = new StreamWriter(client.GetStream());

            var files = Directory.GetFiles(_directory, "*.csv");
            foreach (var file in files)
            {
                var fInfo = new FileInfo(file);

                var secondsFromEpoc = SecondsSinceEpoc(TimeSpan.FromSeconds(_rebaseSeconds));

                var stat = fInfo.Name.Replace(".csv", "");
                var lines = File.ReadAllLines(file).Skip(1);
                foreach (var line in lines)
                {
                    var split = line.Split(",".ToCharArray());

                    var newLine = _rebaseSeconds != 0
                        ? $"{_prefix}.{stat} {split[1]} {secondsFromEpoc}"
                        : $"{_prefix}.{stat} {split[1]} {split[0]} ";

                    if (_rebaseSeconds != 0)
                        secondsFromEpoc += _incrementRebaseSeconds;

                    Console.WriteLine($"Writing {newLine} to Stream (which is: {new DateTime(1970, 1, 1).AddSeconds(secondsFromEpoc):yyyy-MM-dd HH:mm:ss})");
                    stream.WriteLine($"{newLine}");
                }
            }

            stream.Dispose();
            client.Dispose();

            Console.WriteLine("In theory they're all there!");
            Console.ReadLine();
        }

        private static void ShowUsageMessage()
        {
            Console.WriteLine("dotnet run -host localhost -port 2003 -rs 3600 -d c:\\temp\\metrics");
        }


        private static int SecondsSinceEpoc(TimeSpan? hoursToSubtract = null)
        {
            if (hoursToSubtract == null)
                hoursToSubtract = TimeSpan.Zero;

            var t = (DateTime.Now - new DateTime(1970, 1, 1)).Subtract(hoursToSubtract.Value);
            return (int)t.TotalSeconds;
        }
    }
}
