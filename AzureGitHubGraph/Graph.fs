module Graph

type Relationship =
    | UserHasRepo
    | RepoContributedToByUser
    static member TryParse = function
        | "UserHasRepo" -> Some UserHasRepo
        | "RepoContributedToByUser" -> Some RepoContributedToByUser
        | _ -> None

type Item =
    | User of string
    | Repo of owner:string * name:string
    member this.Key =
        match this with
        | User username -> "u_" + username
        | Repo (owner, name) -> "r_" + owner + "^" + name
    static member TryParse str =
        match (str:string).Split '_' with
        | [| "u"; username |] -> Some (User username)
        | [| "r"; repo |] ->
            match repo.Split '^' with
            | [| owner; name |] -> Some (Repo (owner, name))
            | _ -> None
        | _ -> None

type Link =
    | Link of Item * Relationship * Item
    member this.ToKeys =
        let (Link (item1, relationship, item2)) = this
        (sprintf "%s:%A" item1.Key relationship, item2.Key)
    static member FromKeys (partitionKey, rowKey) =
        match (partitionKey:string).Split ':' with
        | [| item1Key; relString |] -> Some (item1Key, relString)
        | _ -> None
        |> Option.bind (fun (item1Key, relString) ->
            match (Item.TryParse item1Key, Relationship.TryParse relString, Item.TryParse rowKey) with
            | Some item1, Some rel, Some item2 -> Some (Link (item1, rel, item2))
            | _ -> None)

let add item1 linkedItems relationship graphTable =
    let keysValues = linkedItems |> Seq.map (fun item2 -> ((Link (item1, relationship, item2)).ToKeys, []))
    Table.insertOrReplaceMany keysValues graphTable |> ignore

let markComplete item completedTable =
    let key = (item:Item).Key
    Table.insertOrReplace key key [] completedTable |> ignore

let isComplete item completedTable =
    let key = (item:Item).Key
    Table.retrieveRow key key completedTable |> Option.isSome