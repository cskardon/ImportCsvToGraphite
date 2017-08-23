# Import Csv To Graphite

The clue is in the title, this imports CSV files to Graphite.

### Why?

Graphite has a 'PlainText' 

### What does this work on?

It's a .NET Core application, so it should work on a Mac, Linux or Windows - I've personally tested on Windows and Linux with no problems

### OK Great, let's do this

1. You need .NET Core which you can get from [here](https://www.microsoft.com/net/core).
   
   1. Install that bad boy
2. Download the source
3. Navigate to the directory (in a console/terminal window)
4. Run `dotnet build`
5. Run it! (see the parameters section if you don't have a default Graphite install)

   1. Windows: `dotnet run -d c:\temp\metrics`
   2. Linux: `dotnet run -d /mnt/c/temp/metrics` 

### Parameters

- [Required] `-d/-directory <DIRECTORY>` the directory the metrics are in
- `-host <HOSTNAME>` - the host the Graphite instance is running on (default: `localhost`)
- `-port <PORT>` - the port for the Graphite PlainText Carbon protocol (default: `2003`)
- `-rs/-rebaseSeconds` - the number of seconds to rebase the log files to, i.e. it will restart all the log entries at `today - rebaseSeconds` (default: `0`)
  
  - If this is `0` then the importer will use the timestamp in the csv files
- `-rsi/-rebaseSecondsIncrement` - the number of seconds to increment for each line in the csv files (default: `5`).
- `-prefix` - the prefix to put in front of the metrics (default: `Neo4j`)

### Works with

* https://github.com/igorborojevic/grafana_neo4j

### Todos

- Better 'Usage' instructions