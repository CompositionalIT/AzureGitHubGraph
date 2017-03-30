#load @"..\.paket\load\main.group.fsx"
#load "Table.fs"
#load "Graph.fs"
#load "GitHub.fs"
#load "GitHubGraph.fs"

open System.IO
open GitHubGraph
open Microsoft.WindowsAzure.Storage.Queue

let storageAccountConnectionString =
    File.ReadAllText(Path.Combine(__SOURCE_DIRECTORY__, @"..\StorageAccountConnectionString.txt")).Trim()

let (graphTable, completedTable, queue) = setup storageAccountConnectionString

// Queue a seed message
queue.AddMessage(CloudQueueMessage "u_theprash")

// Take the next message from the queue and process it
processMessage storageAccountConnectionString

// Current queue length
queue.FetchAttributes(); queue.ApproximateMessageCount

// Delete all data
// deleteData storageAccountConnectionString

GitHub.getRateLimit ()