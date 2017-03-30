module GitHub

open System
open Octokit

let github = GitHubClient(ProductHeaderValue("theprash"))

let getUser name = github.User.Get(name).Result

let getUserRepos name =
    github.Repository.GetAllForUser(name, ApiOptions(PageSize = Nullable 200)).Result
    |> Seq.map (fun r -> r.Owner.Login, r.Name)

let getRepoContributors (owner:string, name:string) =
    github.Repository.GetAllContributors(owner, name, ApiOptions(PageSize = Nullable 200)).Result
    |> Seq.map (fun c -> c.Login)

let getRateLimit () = github.Miscellaneous.GetRateLimits().Result.Rate