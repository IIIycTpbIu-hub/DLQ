namespace DeadLetterQueue.Kafka;

public class RetryOptions : DlqOptions
{
    public string RetryTopic { get; set; }
    
    public int RetryCount { get; set; }
}