using Confluent.Kafka;

namespace DeadLetterQueue.Kafka;

public abstract class ClassicDlqConsumer : BaseDlqConsumer
{
    private readonly DlqOptions _options;

    protected ClassicDlqConsumer(DlqOptions options) : base(options)
    {
        _options = options;
    }
    
    protected override async Task ConsumerLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? consumeResult = null;
            try
            {
                consumeResult = _consumer.Consume(cancellationToken);
                await HandleAsync(consumeResult.Message);
            }
            catch (ConsumeException)
            {
                _consumer.Dispose();
            }
            catch (TaskCanceledException)
            {
                _consumer.Dispose();
            }
            catch (Exception e)
            {
                if (consumeResult == null)
                {
                    break;
                }
                try
                {
                    await _producer.ProduceAsync(_options.DlqTopic, consumeResult.Message);
                }
                catch (ProduceException<string, string> exception)
                {
                    Console.WriteLine($"Unable to send dlq message: {exception}");
                    throw;
                }
            }
            _consumer.Commit(consumeResult);
        }
    }
}