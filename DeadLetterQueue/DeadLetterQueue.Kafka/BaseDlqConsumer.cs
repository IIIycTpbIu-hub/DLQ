using Confluent.Kafka;

namespace DeadLetterQueue.Kafka;

public abstract class BaseDlqConsumer : IDisposable
{
    protected readonly IConsumer<string, string> _consumer;
    protected readonly IProducer<string, string> _producer;
    private readonly CancellationTokenSource _ctx = new();
    private readonly DlqOptions _options;

    public BaseDlqConsumer(DlqOptions options)
    {
        _options = options;
        
        var consumerConfig = new ConsumerConfig()
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroup,
        };
        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetLogHandler((_, m) => Console.WriteLine($"{m.Facility}|{m.Level}|{m.Name}|{m.Message}"))
            .SetErrorHandler((_, e) => Console.WriteLine(e.ToString()))
            .Build();

        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = _options.BootstrapServers,
        };
        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    public async Task StartAsync()
    {
        await Task.Yield();

        var token = _ctx.Token;
        _consumer.Subscribe(_options.MainTopic);

        await Task.Factory.StartNew(
            () => ConsumerLoopAsync(token),
            TaskCreationOptions.LongRunning);
    }

    public Task StopAsync()
    {
        _ctx.Cancel();
        return Task.CompletedTask;
    }

    protected abstract Task ConsumerLoopAsync(CancellationToken cancellationToken);

    protected abstract Task HandleAsync(Message<string, string> message);

    public void Dispose()
    {
        _consumer.Dispose();
        _producer.Dispose();
    }
}