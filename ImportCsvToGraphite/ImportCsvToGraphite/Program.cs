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
        private static bool _convertToDt;
        private static string _convertToDtOutputFolder;
        private static bool _verbose;

        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1);
        
        private static bool AreParametersOk(string[] args)
        {
            for (var i = 0; i < args.Length; i++)
                switch (args[i].ToLowerInvariant())
                {
                    case "-verbose":
                    case "-v":
                        _verbose = true;
                        break;
                    case "-hostname":
                    case "-host":
                        _hostname = args[++i];
                        break;
                    case "-port":
                        if (int.TryParse(args[++i], out var port))
                            _port = port;
                        // ReSharper disable once NotResolvedInText
                        else throw new ArgumentOutOfRangeException("port", args[i], "Port isn't a number!");
                        break;
                    case "-directory":
                    case "-d":
                        _directory = args[++i];
                        break;
                    case "-rebaseseconds":
                    case "-rs":
                        if (int.TryParse(args[++i], out var rebaseSeconds)) _rebaseSeconds = rebaseSeconds;
                        else
                            // ReSharper disable once NotResolvedInText
                            throw new ArgumentOutOfRangeException("rebaseSeconds", args[i],
                                "Seconds provided for RebaseSeconds wasn't a number");
                        break;
                    case "-rebasesecondsincrement":
                    case "-rsi":
                        if (int.TryParse(args[++i], out var rebaseIncrement)) _incrementRebaseSeconds = rebaseIncrement;
                        else
                            // ReSharper disable once NotResolvedInText
                            throw new ArgumentOutOfRangeException("rebaseIncrementSeconds", args[i],
                                "Seconds provided to increment RebaseSeconds wasn't a number.");
                        break;
                    case "-prefix":
                        _prefix = args[++i];
                        break;
                    case "-converttodt":
                        _convertToDt = true;
                        _convertToDtOutputFolder = args[++i];
                        break;
                    default:
                        Console.WriteLine($"Unknown parameter supplied: '{args[i]}'");
                        ShowUsageMessage();
                        return false;
                }

            if (!_convertToDt && (string.IsNullOrWhiteSpace(_hostname) || string.IsNullOrWhiteSpace(_prefix) ||
                                  string.IsNullOrWhiteSpace(_directory)))
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

            if (_convertToDt)
                ConvertToDateTime();
            else
                ImportToGraphite();

            Console.WriteLine("All done!!");
            Console.WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }

        private static void ImportToGraphite()
        {
            var client = new TcpClient();
            client.Connect(_hostname, _port);
            Console.WriteLine($"Connected to the Graphite end point ({client.Connected}).");

            var stream = new StreamWriter(client.GetStream());

            var files = Directory.GetFiles(_directory, "*.csv");
            foreach (var file in files)
            {
                var fInfo = new FileInfo(file);
                Console.WriteLine($"Inserting {fInfo.Name} to Graphite....");
                var secondsSinceEpoch = SecondsSinceEpoch(TimeSpan.FromSeconds(_rebaseSeconds));

                var stat = fInfo.Name.Replace(".csv", "");
                var lines = File.ReadAllLines(file).Skip(1);
                foreach (var line in lines)
                {
                    if (line.StartsWith("t,"))
                        continue;

                    var split = line.Split(",".ToCharArray());

                    var newLine = _rebaseSeconds != 0
                        ? $"{_prefix}.{stat} {split[1]} {secondsSinceEpoch}"
                        : $"{_prefix}.{stat} {split[1]} {split[0]} ";

                    if (_rebaseSeconds != 0)
                        secondsSinceEpoch += _incrementRebaseSeconds;

                    if (_verbose)
                        Console.WriteLine($"\tWriting {newLine} to Stream (which is: {new DateTime(1970, 1, 1).AddSeconds(secondsSinceEpoch):yyyy-MM-dd HH:mm:ss})");
                    stream.WriteLine($"{newLine}");
                }

                Console.WriteLine($"Inserted {fInfo.Name} to Graphite.");
            }

            stream.Dispose();
            client.Dispose();
        }

        private static void ConvertToDateTime()
        {
            var files = Directory.GetFiles(_directory, "*.csv");
            var outputDirectory = string.IsNullOrWhiteSpace(_convertToDtOutputFolder) ? new DirectoryInfo(_directory) : Directory.CreateDirectory(_convertToDtOutputFolder);

            foreach (var file in files)
            {
                var inputFile = new FileInfo(file);
                var outputFile = File.Create(Path.Combine(outputDirectory.FullName, inputFile.Name));

                var stat = inputFile.Name.Replace(".csv", "");

                var start = DateTime.Now;
                Console.WriteLine($"Writing {inputFile.Name} to {outputFile.Name}.");

                var outputWriter = new StreamWriter(outputFile);

                outputWriter.WriteLine($"DateTime,{_prefix}.{stat}");
                var secondsSinceEpoch = SecondsSinceEpoch(TimeSpan.FromSeconds(_rebaseSeconds));

                var lines = File.ReadAllLines(file).Skip(1);
                foreach (var line in lines)
                {
                    if (line.StartsWith("t,"))
                        continue;

                    var split = line.Split(",".ToCharArray());

                    var newLine = _rebaseSeconds != 0
                        ? $"{UnixEpoch.AddSeconds(secondsSinceEpoch)},{split[1]}"
                        : $"{UnixEpoch.AddSeconds(Convert.ToInt32(split[0]))},{split[1]}";

                    if (_rebaseSeconds != 0)
                        secondsSinceEpoch += _incrementRebaseSeconds;

                    if (_verbose)
                        Console.WriteLine($"Writing {newLine} to {outputFile.Name} (which is: {new DateTime(1970, 1, 1).AddSeconds(secondsSinceEpoch):yyyy-MM-dd HH:mm:ss})");

                    outputWriter.WriteLine(newLine);
                }
                outputFile.Close();
                
                Console.WriteLine($"Wrote {inputFile.Name} to {outputFile.Name} ({(DateTime.Now - start).TotalMilliseconds} ms).");
            }
        }


        private static void ShowUsageMessage()
        {
            Console.WriteLine("Importer / Converter");
            Console.WriteLine("usage: dotnet run -d DIRECTORY [-host HOST -port PORT] [-convertToDt FOLDER] [-rs REBASE] [-rsi REBASE_INCREMENT] [-prefix PREFIX] [-verbose]");
            Console.WriteLine();
            Console.WriteLine("Args:");
            Console.WriteLine("-d / -directory :: The directory to read the metrics files from [REQUIRED]");
            Console.WriteLine("-host HOST :: the host of the graphite server (IP/DNS name) [OPTIONAL]");
            Console.WriteLine("-port PORT :: the port of the graphite server [OPTIONAL - REQUIRED if using '-host']");
            Console.WriteLine("-prefix :: a prefix to append to the stream to graphite to organise your metrics (typically something like 'neo4j') [OPTIONAL]");
            Console.WriteLine("-convertToDt FOLDER :: the folder to output converted CSV files to. [OPTIONAL]");
            Console.WriteLine("-rs REBASE :: the point to rebase the metrics files from in seconds [OPTIONAL]");
            Console.WriteLine("-rsi REBASE INCREMENT :: the number of seconds to incremement each metric by when reading in the data [OPTIONAL]");
            Console.WriteLine("-verbose :: Outputs verbose info about things happening (due to writing to the screen this will significantly slow down the process).");

            Console.WriteLine("---------------------------------------------------------------------------");
            Console.WriteLine("Import to Graphite Example:");
            Console.WriteLine("dotnet run -host localhost -port 2003 -rs 3600 -d c:\\temp\\metrics");
            Console.WriteLine();
            Console.WriteLine("Convert To DT example:");
            Console.WriteLine("dotnet run -d c:\\temp\\metrics -convertToDt c:\\temp\\metrics\\dt -verbose");
        }


        private static int SecondsSinceEpoch(TimeSpan? hoursToSubtract = null)
        {
            if (hoursToSubtract == null)
                hoursToSubtract = TimeSpan.Zero;

            var t = (DateTime.Now - new DateTime(1970, 1, 1)).Subtract(hoursToSubtract.Value);
            return (int) t.TotalSeconds;
        }
    }
}