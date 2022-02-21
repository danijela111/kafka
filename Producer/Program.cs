using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Net;

namespace consumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<ConsoleHostedService>();
                });
    }

    internal sealed class ConsoleHostedService : IHostedService
    {
        private readonly ILogger _logger;
        private readonly IHostApplicationLifetime _appLifetime;
        private bool _stopRequested = false;
        private ProducerConfig _config;

        public ConsoleHostedService(
            ILogger<ConsoleHostedService> logger,
            IHostApplicationLifetime appLifetime)
        {
            _logger = logger;
            _appLifetime = appLifetime;

            _config = new ProducerConfig
            {
                BootstrapServers = "kafka-1:19093,kafka-2:29093,kafka-3:39093",
                ClientId = Dns.GetHostName(),
            };
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug($"Starting with arguments: {string.Join(" ", Environment.GetCommandLineArgs())}");

            _appLifetime.ApplicationStarted.Register(() =>
            {
                Task.Run(async () =>
                {
                    try
                    {
                        using var producer = new ProducerBuilder<Null, string>(_config).Build();
                        _logger.LogInformation("Hello World!");

                        // Simulate real work is being done
                        var counter = 0;
                        while (counter < 1)
                        {

                            await producer.ProduceAsync("exampleTopic", new Message<Null, string> { Value = "test1" });
                            await producer.ProduceAsync("exampleTopic", new Message<Null, string> { Value = "test2" });
                            await producer.ProduceAsync("exampleTopic", new Message<Null, string> { Value = "test3" });
                            await producer.ProduceAsync("exampleTopic", new Message<Null, string> { Value = "test4" });
                            await producer.ProduceAsync("exampleTopic", new Message<Null, string> { Value = "test5" });
      

                            counter++;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unhandled exception!");
                    }
                    finally
                    {
                        // Stop the application once the work is done
                        _appLifetime.StopApplication();
                    }
                });
            });

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _stopRequested = true;
            return Task.CompletedTask;
        }

    }
}
