// See https://aka.ms/new-console-template for more information

using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

Console.WriteLine("Hello, World!");

var cts = new CancellationTokenSource();
var host = "localhost";

try
{
    Task.Run(() => ProducerLoop(host, cts.Token));
    Task.Run(() => ConsumerLoop(host, cts.Token));
}
catch (Exception e)
{
    Console.WriteLine(e);
    throw;
}

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();
cts.Cancel();

async Task ProducerLoop(string host, CancellationToken cancellationToken)
{
    var factory = new ConnectionFactory()
    {
        HostName = host,
        Port = 5672,
        UserName = "rmuser",
        Password = "rmpassword",
    };
    
    Console.WriteLine("Starting producer");
    using var connection = factory.CreateConnection();

    using var channel = connection.CreateModel();

    channel.QueueDeclare("test-queue", false, false, false);
    int count = 0;
    
    var body = "iteration"u8.ToArray();

    Console.WriteLine("Enable sending loop");
    while (!cancellationToken.IsCancellationRequested)
    {
        await Task.Delay(3000);
        
        channel.BasicPublish(exchange: string.Empty,
            routingKey: "test-queue",
            basicProperties: null,
            body: body);
        Console.WriteLine($" [x] message sent");
    }
}

async Task ConsumerLoop(string host, CancellationToken cancellationToken)
{
    var factory = new ConnectionFactory()
    {
        HostName = host,
        Port = 5672,
        UserName = "rmuser",
        Password = "rmpassword",
    };
    
    Console.WriteLine("Starting consumer");
    using var connection = factory.CreateConnection();

    using var channel = connection.CreateModel();
    
    channel.QueueDeclare("test-queue", false, false, false);

    var consumer = new EventingBasicConsumer(channel);

    consumer.Received += (sender, eventArgs) =>
    {
        var body = eventArgs.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);

        Console.WriteLine($"Consumed: {message}");
    };

    channel.BasicConsume("test-queue", true, consumer);

    while (!cancellationToken.IsCancellationRequested)
    {
        await Task.Delay(1000);
    }
}