using System.Text;
using Confluent.Kafka;

namespace DeadLetterQueue.Kafka;

public abstract class RetryDlqConsumer : BaseDlqConsumer
{
    private readonly RetryOptions _options;
    
    public RetryDlqConsumer(RetryOptions options) : base(options)
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
                _consumer.Commit(consumeResult);
                await HandleAsync(consumeResult.Message);
            }
            //other important catch blocks
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
                    var retryNumber = 0;

                    if (consumeResult.Message.Headers.TryGetLastBytes("x-retry-number", out var bytes))
                    {
                        retryNumber = Int32.Parse(Encoding.UTF8.GetString(bytes));
                    }

                    if (retryNumber < _options.RetryCount)
                    {
                        retryNumber++;
                        consumeResult.Message.Headers.Remove("x-retry-number");
                        consumeResult.Message.Headers.Add("x-retry-number", Encoding.UTF8.GetBytes(retryNumber.ToString()));
                        await _producer.ProduceAsync(_options.RetryTopic, consumeResult.Message);
                    }
                    else
                    {
                        await _producer.ProduceAsync(_options.DlqTopic, consumeResult.Message);
                    }
                }
                catch (ProduceException<string, string> exception)
                {
                    Console.WriteLine($"Unable to send dlq message: {exception}");
                }
            }
        }
    }
}