namespace Web.Host

open Microsoft.Extensions.Hosting
open System.Threading.Tasks
open System.Collections.Generic
open System
open System.Collections.Concurrent
open System.Net
open System.Text
open FSharp.Control.Tasks.V2.ContextInsensitive
open Microsoft.Extensions.Logging
open Pulsar.Client.Api
open Pulsar.Client.Common
open System.Threading

type PulsarConsumer(pulsarClient: PulsarClient, Log: ILogger, errorConsuming, hostName: string, handler: Message<String> -> unit,
                    commandTopic: string) =
    inherit BackgroundService()
    do Log.LogInformation("Init PulsarConsumer {0}", hostName)

    let configurePulsar prefix =
        pulsarClient.NewConsumer(Schema.STRING())
            .Topic(prefix + "output")
            .ConsumerName(prefix + " consumer")
            .SubscriptionName(hostName)
            .SubscriptionType(SubscriptionType.Exclusive)
            .SubscriptionMode(SubscriptionMode.NonDurable)
            .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .SubscribeAsync()

    let runConsumer (consumer: IConsumer<String>) (cancellationToken: CancellationToken) =
        task {
            Log.LogInformation("ExecuteAsync PulsarConsumer. {0} Processing...", hostName)
            try
                while (cancellationToken.IsCancellationRequested |> not) do
                    let mutable success = false
                    let! message = consumer.ReceiveAsync()
                    try
                        do handler message
                        success <- true
                    with
                    | ex ->
                        Log.LogError(ex, "Can't process message {Topic} {Key} {Value}", consumer.Topic, message.Key, Encoding.UTF8.GetString(message.Data))
                        do! consumer.NegativeAcknowledge(message.MessageId)
                    if success then
                        do! consumer.AcknowledgeAsync(message.MessageId)
            with
                | ex ->
                    Log.LogError(ex, "ReceiveAsync failed for {Topic}", consumer.Topic)
        }
        
    override this.ExecuteAsync cancellationToken =
        task {
            let! consumer = configurePulsar commandTopic
            let! _  = Task.WhenAny([|
                runConsumer consumer cancellationToken           
            |])
            errorConsuming()
        } :> Task

