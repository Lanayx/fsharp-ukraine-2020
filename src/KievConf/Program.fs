module KievConf.App

open System
open System.Collections.Concurrent
open System.IO
open System.Net
open System.Text
open System.Threading.Tasks
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Cors.Infrastructure
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Giraffe
open Microsoft.Extensions.Hosting
open Pulsar.Client.Api
open Web.Host
open FSharp.Control.Tasks.V2.ContextInsensitive
open Microsoft.AspNetCore.Http
open Pulsar.Client.Common
open FSharp.UMX

let client = PulsarClientBuilder().ServiceUrl("pulsar://my-pulsar-cluster:30002").Build()
let commandTopic = Dns.GetHostName() + "InventoryCommand"

// fake Reader creates topic to be picked up by regex function 
let fakeReader = 
    client.NewReader(Schema.STRING())
        .ReaderName("Fake reader")
        .StartMessageId(MessageId.Earliest)
        .Topic(commandTopic)
        .CreateAsync().GetAwaiter().GetResult()

let producer = 
    client.NewProducer(Schema.STRING())
        .ProducerName(Dns.GetHostName() + "Command producer")
        .Topic(commandTopic)
        .CreateAsync().GetAwaiter().GetResult()
        
let requests = ConcurrentDictionary<string, TaskCompletionSource<Message<String>>>()

// ---------------------------------
// Models
// ---------------------------------

type ViewModel =
    {
        ProductId: int
        Count : string
    }

// ---------------------------------
// Views
// ---------------------------------


module Views =
    open GiraffeViewEngine

    let layout (content: XmlNode list) =
        html [] [
            head [] [
                title []  [ encodedText "KievConf" ]
                link [ _rel  "stylesheet"
                       _type "text/css"
                       _href "/main.css" ]
            ]
            body [] content
        ]

    let index (model : ViewModel) =
        let mutable count = 0;
        
        [
            h1 [] [ encodedText "KievConf" ]
            h1 [] [ encodedText (sprintf "Product %i" model.ProductId)  ]
            if (Int32.TryParse(model.Count, &count)) then
                p [] [ encodedText (sprintf "Inventory count: %i" count) ]
            else
                p [ _style "color: red" ] [ encodedText "Error!" ]
            form [ _action (sprintf "/add/%i" model.ProductId); _method "POST" ] [
                input [ _type "number"; _name "count" ]
                button [ _type "submit" ] [ encodedText "Add" ]
            ]
            
        ] |> layout

// ---------------------------------
// Web app
// ---------------------------------

let getHandler (productId: int) next (ctx: HttpContext) =
    task {
        let data = sprintf "GET|%i" productId
        let key = Guid.NewGuid().ToString("N")
        let! msgId = producer.SendAsync(producer.NewMessage(data, key))
        let tcs = TaskCompletionSource<Message<String>>(TaskCreationOptions.RunContinuationsAsynchronously)
        requests.TryAdd(key, tcs) |> ignore
        let! message = tcs.Task
        let result = message.GetValue()
        let model = { ProductId = productId; Count = result }
        let view = Views.index model
        return! htmlView view next ctx
    }
    

let addHandler (productId: int) next (ctx: HttpContext) =
    task {
        let count = ctx.GetFormValue("count").Value |> int
        let data = sprintf "ADD|%i|%i" productId count
        let key = Guid.NewGuid().ToString("N")
        let! msgId = producer.SendAsync(producer.NewMessage(data, key))
        let tcs = TaskCompletionSource<Message<String>>(TaskCreationOptions.RunContinuationsAsynchronously)
        requests.TryAdd(key, tcs) |> ignore
        let! message = tcs.Task
        let result = Encoding.UTF8.GetString(message.Data)
        let model = { ProductId = productId; Count = result }
        let view = Views.index model
        return! htmlView view next ctx
    }

let webApp =
    choose [
        GET >=>
            choose [
                route "/" >=> text "Hello world"
                routef "/get/%i" getHandler
            ]
        POST >=>
            choose [
                routef "/add/%i" addHandler
            ]
        setStatusCode 404 >=> text "Not Found" ]

// ---------------------------------
// Error handler
// ---------------------------------

let errorHandler (ex : Exception) (logger : ILogger) =
    logger.LogError(ex, "An unhandled exception has occurred while executing the request.")
    clearResponse >=> setStatusCode 500 >=> text ex.Message

// ---------------------------------
// Config and Main
// ---------------------------------

let configureCors (builder : CorsPolicyBuilder) =
    builder.WithOrigins("http://localhost:8080")
           .AllowAnyMethod()
           .AllowAnyHeader()
           |> ignore

let configureApp (app : IApplicationBuilder) =
    let env = app.ApplicationServices.GetService<IHostEnvironment>()
    (match env.IsDevelopment() with
    | true  -> app.UseDeveloperExceptionPage()
    | false -> app.UseGiraffeErrorHandler errorHandler)
        .UseHttpsRedirection()
        .UseCors(configureCors)
        .UseStaticFiles()
        .UseGiraffe(webApp)

let handler (message: Message<String>) =
    Console.WriteLine(Encoding.UTF8.GetString(message.Data) + " received")
    match requests.TryGetValue(%message.Key) with
    | true, tcs -> tcs.SetResult(message)
    | _ -> ()

let configureServices (services : IServiceCollection) =
    services.AddCors()    |> ignore
    services.AddGiraffe() |> ignore
    services.AddSingleton<IHostedService, PulsarConsumer>(fun (sp) ->
            let appLifetime = sp.GetService<IHostApplicationLifetime>()
            let logger = sp.GetService<ILogger<PulsarConsumer>>()
            let host = Dns.GetHostName()
            let stopApp() =
                logger.LogCritical("{0} stopped consuming", host)
                client.CloseAsync().GetAwaiter().GetResult()
                appLifetime.StopApplication()
            new PulsarConsumer(client, logger, stopApp, host, handler, commandTopic)
        ) |> ignore

let configureLogging (builder : ILoggingBuilder) =
    builder.AddFilter(fun l -> l >= LogLevel.Information)
           .AddConsole()
           .AddDebug() |> ignore

[<EntryPoint>]
let main _ =
    let contentRoot = Directory.GetCurrentDirectory()
    let webRoot     = Path.Combine(contentRoot, "WebRoot")
    WebHostBuilder()
        .UseKestrel()
        .UseContentRoot(contentRoot)
        .UseIISIntegration()
        .UseWebRoot(webRoot)
        .Configure(Action<IApplicationBuilder> configureApp)
        .ConfigureServices(configureServices)
        .ConfigureLogging(configureLogging)
        .Build()
        .Run()
    0