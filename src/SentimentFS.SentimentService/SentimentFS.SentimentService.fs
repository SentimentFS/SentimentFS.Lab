namespace SentimentFS.SentimentService

open Akkling
open System
open System.Net.Http
open Newtonsoft.Json
open System.Collections.Generic
open System.Text

module Messages =
    open SentimentFS.NaiveBayes.Dto

    type Emotion =
        | VeryNegative = -2
        | Negative = -1
        | Neutral = 0
        | Positive = 1
        | VeryPositive = 2

    [<CLIMutable>]
    type TrainRequest = { text: string; category: Emotion; weight : int }


    type SentimentServiceMessages = 
        | Train of TrainRequest * string

module Program =
    open Messages
    open Akka.Actor
    open Akkling.Streams

    let private intToEmotion (value: int): Emotion =
        match value with
        | -5 | -4 -> Emotion.VeryNegative
        | -3 | -2 | -1 -> Emotion.Negative
        | 0 -> Emotion.Neutral
        | 1 | 2 | 3 -> Emotion.Positive
        | 4 | 5 -> Emotion.VeryPositive
        | _ -> Emotion.Neutral

    let httpClient = HttpClient();

    let actor _ = function
        | Train (req, url) ->
            use content = new StringContent((req |> JsonConvert.SerializeObject), UTF8Encoding.UTF8, "application/json")
            async {
                return! httpClient.PutAsync(url, content) |> Async.AwaitTask
            } |> Async.RunSynchronously |> ignored

    let initClassifier apiUrl tainingDataUrl (actor: IActorRef<SentimentServiceMessages>) =
        let httpResult = async {
                let! result = httpClient.GetAsync(System.Uri(tainingDataUrl)) |> Async.AwaitTask
                result.EnsureSuccessStatusCode() |> ignore
                return! result.Content.ReadAsStringAsync() |> Async.AwaitTask } |> Async.RunSynchronously

        let emotions = httpResult |> JsonConvert.DeserializeObject<IDictionary<string, int>>

        for keyValue in emotions do
            actor <! (Train(({ text = keyValue.Key; category = keyValue.Value |> intToEmotion; weight = 1 } , apiUrl)))

    let traingDataSource trainingDataUrl = 
        Source.ofAsync(async {
                let! result = httpClient.GetAsync(System.Uri(trainingDataUrl)) |> Async.AwaitTask
                result.EnsureSuccessStatusCode() |> ignore
                let! json = result.Content.ReadAsStringAsync() |> Async.AwaitTask 
                return json |> JsonConvert.DeserializeObject<IDictionary<string, int>> })

    [<EntryPoint>]
    let main argv =
        let system = System.create "sentimentfs" <| Configuration.load()
        let actor = spawn system "sentiment" <| props (actorOf2 (actor))
        initClassifier "http://localhost:5000/api/sentiment/trainer" "https://raw.githubusercontent.com/wooorm/afinn-96/master/index.json" actor
        Console.ReadKey();
        0 // return an integer exit code
