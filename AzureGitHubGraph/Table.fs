module Table

open Microsoft.WindowsAzure.Storage.Table

let makeEntity partitionKey rowKey properties = 
    let entity = DynamicTableEntity(partitionKey, rowKey)
    for (prop, value) in properties do
        entity.[prop] <- EntityProperty.CreateEntityPropertyFromObject value
    entity

/// Pass a maximum of 100 values
let insertOrReplaceManyUnsafe keysValues table =
    let batch = TableBatchOperation()
    for (partitionKey, rowKey, properties) in keysValues do
        batch.InsertOrReplace(makeEntity partitionKey rowKey properties)
    (table:CloudTable).ExecuteBatch batch

let insertOrReplaceMany keysValues table =
    keysValues
    |> Seq.toArray
    |> Array.chunkBySize 100
    |> Array.Parallel.map (fun kvs -> insertOrReplaceManyUnsafe kvs table)
    |> Array.collect Seq.toArray

let insertOrReplace partitionKey rowKey properties table = 
    makeEntity partitionKey rowKey properties
    |> TableOperation.InsertOrReplace
    |> (table:CloudTable).Execute

let delete partitionKey rowKey table =
    TableOperation.Delete(DynamicTableEntity(partitionKey, rowKey, ETag = "*")) |> (table:CloudTable).Execute

let retrieveRow partitionKey rowKey table =
    TableOperation.Retrieve<DynamicTableEntity>(partitionKey, rowKey) |> (table:CloudTable).Execute
    |> fun r -> r.Result
    |> function
        | :? DynamicTableEntity as out -> Some out
        | _ -> None