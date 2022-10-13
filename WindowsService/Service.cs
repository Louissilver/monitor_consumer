using System;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Configuration;
using System.Threading;
using RabbitMQ.Client;
using System.Net.NetworkInformation;
using System.Net;
using System.Net.Sockets;
using RabbitMQ.Client.Events;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;

namespace MonitoringConsumer
{
    [RunInstaller(true)]
    public partial class Service : ServiceBase
    {
        readonly int ScheduleTime = Convert.ToInt32(ConfigurationManager.AppSettings["ThreadTime"]);

        private Thread Worker = null;

        public Service()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            ThreadStart start = new ThreadStart(Working);
            Worker = new Thread(start);
            Worker.Start();
        }

        public void Working()
        {
            int idMessage = 0;
            Console.WriteLine("Aguardando novas mensagens.\n\n");
            while (true)
            {
                string cs = @"server=localhost;port=3306;userid=root;password=Kcto201933!!;database=sistdist";
                var con = new MySqlConnection(cs);
                con.Open();

                var cmd = new MySqlCommand();
                cmd.Connection = con;

                var factory = new ConnectionFactory() { HostName = "localhost" };
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "hello",
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);

                        PayloadMaquina weatherForecast =
                         JsonConvert.DeserializeObject<PayloadMaquina>(message);

                        cmd.CommandText = $"INSERT INTO maquina (macAddress, ipv4, hostname, datahora, local) VALUES(\"{ weatherForecast?.macAddress}\", \"{ weatherForecast?.ipv4}\", \"{ weatherForecast?.hostname}\", \"{weatherForecast?.DataHoraMensagem}\", \"{ weatherForecast?.local}\");";
                        cmd.ExecuteNonQuery();
                        idMessage += 1;
                    };
                    channel.BasicConsume(queue: "hello",
                                         autoAck: true,
                                         consumer: consumer);
                }
                Thread.Sleep(ScheduleTime * 60 * 1000);
            }
        }

        protected override void OnStop()
        {
            if (Worker != null && Worker.IsAlive)
            {
                Worker.Abort();
            }
        }

        public class PayloadMaquina
        {
            public string macAddress { get; set; }
            public string ipv4 { get; set; }
            public string hostname { get; set; }
            public string DataHoraMensagem { get; set; }
            public string local { get; set; }
        }
    }
}
