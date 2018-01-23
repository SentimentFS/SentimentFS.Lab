namespace SentimentFS.SentimentService

open Akkling
open System
open System.Net.Http
open Newtonsoft.Json
open System.Collections.Generic
open System.Text
open System.Linq
open Akka.Streams.Dsl

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

    let traingDataSource trainingDataUrl = 
        Source.ofAsync(async {
                let! result = httpClient.GetAsync(System.Uri(trainingDataUrl)) |> Async.AwaitTask
                result.EnsureSuccessStatusCode() |> ignore
                let! json = result.Content.ReadAsStringAsync() |> Async.AwaitTask 
                return json |> JsonConvert.DeserializeObject<IDictionary<string, int>> })

    let mapRequestFlow(apiUrl: string) = 
        Flow.id
        |> Flow.collect(fun (dict: IDictionary<string, int>) -> dict.ToList())
        |> Flow.asyncMap(10)(fun x -> 
                                use content = new StringContent(({ text = x.Key; category = x.Value |> intToEmotion; weight = 1 } |> JsonConvert.SerializeObject), UTF8Encoding.UTF8, "application/json")
                                async {
                                    return! httpClient.PutAsync(apiUrl, content) |> Async.AwaitTask
                                }
                            )
    

    [<EntryPoint>]
    let main argv =
        let system = System.create "sentimentfs" <| Configuration.load()
        let s = traingDataSource("https://raw.githubusercontent.com/wooorm/afinn-96/master/index.json") 
                |> Source.via(mapRequestFlow("http://localhost:5000/api/sentiment/trainer"))

        async {
           do! s |>Source.runWith (system.Materializer()) (Sink.ignore)
        } |> Async.RunSynchronously
              
        Console.ReadKey();
        0 // return an integer exit code
