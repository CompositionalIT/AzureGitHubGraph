module GitHubGraph

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Queue

let setup storageAccountConnectionString =
    let azureStorageAccount = CloudStorageAccount.Parse storageAccountConnectionString
    let tableClient = azureStorageAccount.CreateCloudTableClient()
    let queueClient = azureStorageAccount.CreateCloudQueueClient()

    let graphTable = tableClient.GetTableReference("GitHubGraph")
    let completedTable = tableClient.GetTableReference("GitHubCompletedIds")
    let queue = queueClient.GetQueueReference("queue")

    graphTable.CreateIfNotExists() |> ignore
    completedTable.CreateIfNotExists() |> ignore
    queue.CreateIfNotExists() |> ignore
    graphTable, completedTable, queue

let deleteData storageAccountConnectionString =
    let (graphTable, completedTable, queue) = setup storageAccountConnectionString
    graphTable.DeleteIfExistsAsync() |> Async.AwaitTask |> Async.RunSynchronously |> ignore
    completedTable.DeleteIfExistsAsync() |> Async.AwaitTask |> Async.RunSynchronously |> ignore
    queue.ClearAsync() |> Async.AwaitTask |> Async.RunSynchronously

let processMessage storageAccountConnectionString =
    let (graphTable, completedTable, queue) = setup storageAccountConnectionString
    let messageAlreadyProcessed m = completedTable |> Table.retrieveRow m m |> Option.isSome

    let rec getMessage () =
        let message = queue.GetMessage()
        if message = null then failwith "No messages in queue"
        if messageAlreadyProcessed message.AsString
        then
            queue.DeleteMessage message
            printfn "Already processed %s. Getting another queue message..." message.AsString
            getMessage ()
        else message

    let message = getMessage ()
    let m = message.AsString

    printfn "Processing %s..." m

    let item =
        match Graph.Item.TryParse m with
        | Some i -> i
        | None -> failwith "Invalid message"
    let (linkedItems, linkType) =
        match item with
        | Graph.User username ->
            GitHub.getUserRepos username |> Seq.map Graph.Repo, Graph.UserHasRepo
        | Graph.Repo (owner, name) ->
            GitHub.getRepoContributors (owner, name) |> Seq.map Graph.User, Graph.RepoContributedToByUser

    Graph.add item linkedItems linkType graphTable
    Graph.markComplete item completedTable

    queue.DeleteMessage message

    linkedItems
    |> Seq.map (fun i ->
        let key = i.Key
        printfn "Adding %s to queue." key
        key |> CloudQueueMessage |> queue.AddMessageAsync |> Async.AwaitTask)
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore