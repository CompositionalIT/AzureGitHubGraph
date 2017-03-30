module Graph

type Relationship =
    | UserHasRepo
    | RepoContributedToByUser

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

let add item linkedItems linkType graphTable =
    let partitionKey = sprintf "%s:%A" (item:Item).Key linkType
    let keysValues = linkedItems |> Seq.map (fun i -> (partitionKey, (i:Item).Key, []))
    Table.insertOrReplaceMany keysValues graphTable |> ignore

let markComplete item completedTable =
    let key = (item:Item).Key
    Table.insertOrReplace key key [] completedTable |> ignore

let isComplete item completedTable =
    let key = (item:Item).Key
    Table.retrieveRow key key completedTable |> Option.isSome