using Confluent.Kafka;
using System;

namespace consumer
{
    class Program
    {
        static void Main(string[] args)
        {

            using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
            {
                BootstrapServers = "kafka-1:19093,kafka-2:29093,kafka-3:39093",
                GroupId = "TestGroup",
                AutoOffsetReset = AutoOffsetReset.Earliest
            }).Build();
            {
                consumer.Subscribe("exampleTopic");

                while (true)
                {
                    var consumeResult = consumer.Consume(new CancellationToken());
                    Console.WriteLine(consumeResult.Message.Value);
                }
            }
        }
    }
}
