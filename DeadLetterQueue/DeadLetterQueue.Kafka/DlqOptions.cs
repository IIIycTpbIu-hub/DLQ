namespace DeadLetterQueue.Kafka;

public class DlqOptions
{
    public string BootstrapServers { get; set; }

    public string ConsumerGroup { get; set; }

    public string MainTopic { get; init; }

    public string DlqTopic { get; init; }
}