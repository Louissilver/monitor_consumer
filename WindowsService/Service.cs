using System;
using System.ComponentModel;
using System.ServiceProcess;
using System.Text;
using System.Configuration;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace MonitoringConsumer
{
    [RunInstaller(true)]
    public partial class Service : ServiceBase
    {
        readonly int ScheduleTime = Convert.ToInt32(ConfigurationManager.AppSettings["ThreadTime"]);
        readonly string ConnectionString = ConfigurationManager.AppSettings["ConnectionString"];

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
            while (true)
            {
                List<PayloadMaquina> macAddressPayload = new List<PayloadMaquina>();
                var factory = new ConnectionFactory() { HostName = "localhost" };
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "monitor",
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);

                        PayloadMaquina payload =
                         JsonConvert.DeserializeObject<PayloadMaquina>(message);
                        macAddressPayload.Add(payload);
                    };
                    channel.BasicConsume(queue: "monitor",
                                         autoAck: true,
                                         consumer: consumer);
                }

                var con = new MySqlConnection(ConnectionString);
                con.Open();

                foreach (var a in macAddressPayload)
                {
                    var sql = $"SELECT COUNT(id) FROM horarios WHERE macAddress = \"{a.macAddress}\" AND data = CURDATE()";
                    var cmd = new MySqlCommand(sql, con);
                    MySqlDataReader rdr = cmd.ExecuteReader();
                    rdr.Read();

                    if (rdr.GetInt32(0) == 0)
                    {
                        rdr.Close();
                        cmd = new MySqlCommand();
                        cmd.Connection = con;
                        cmd.CommandText = $"INSERT INTO horarios (macAddress, hora_inicial, hora_final, data) VALUES (\"{a.macAddress}\", \"{a.hora}\", \"{DateTime.Now.ToString("HH:mm:ss")}\", \"{a.data}\"); INSERT INTO computadores (macAddress, ipv4, hostname, local) VALUES (\"{a.macAddress}\", \"{a.ipv4}\", \"{a.hostname}\", \"{a.local}\");";
                        cmd.ExecuteNonQuery();

                    }
                    else
                    {
                        rdr.Close();
                        sql = $"SELECT hora_final FROM horarios WHERE macAddress = \"{a.macAddress}\" AND data = CURDATE() ORDER BY id DESC LIMIT 1";
                        cmd = new MySqlCommand(sql, con);
                        rdr = cmd.ExecuteReader();
                        if (rdr.Read())
                        {
                            if (DateTime.Parse(rdr.GetString(0)).AddMinutes(10) < DateTime.Now || rdr.GetString(0) == "")
                            {
                                rdr.Close();
                                cmd = new MySqlCommand();
                                cmd.Connection = con;
                                cmd.CommandText = $"INSERT INTO horarios (macAddress, hora_inicial, hora_final, data) VALUES (\"{a.macAddress}\", \"{a.hora}\", \"{DateTime.Now.ToString("HH:mm:ss")}\", \"{a.data}\")";
                                cmd.ExecuteNonQuery();
                            }
                            else
                            {
                                rdr.Close();
                                cmd = new MySqlCommand();
                                cmd.Connection = con;
                                cmd.CommandText = $"UPDATE horarios SET hora_final = \"{a.hora}\" WHERE macAddress = '{a.macAddress}' AND data = CURDATE() ORDER BY id DESC LIMIT 1";
                                cmd.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            rdr.Close();
                            cmd = new MySqlCommand();
                            cmd.Connection = con;
                            cmd.CommandText = $"INSERT INTO horarios (macAddress, hora_inicial, hora_final, data) VALUES (\"{a.macAddress}\", \"{a.hora}\", \"{DateTime.Now.ToString("HH:mm:ss")}\", \"{a.data}\")";
                            cmd.ExecuteNonQuery();
                        }

                    }
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
            public string data { get; set; }
            public string hora { get; set; }
            public string local { get; set; }
        }
    }
}
