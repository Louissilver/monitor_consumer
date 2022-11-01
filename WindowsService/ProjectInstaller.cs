using System.ComponentModel;
using System.Configuration.Install;

namespace MonitoringConsumer
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : System.Configuration.Install.Installer
    {
        public ProjectInstaller()
        {
            InitializeComponent();
        }

        private void serviceProcessInstaller1_AfterInstall(object sender, InstallEventArgs e)
        {

        }

        private void serviceInstaller2_AfterInstall(object sender, InstallEventArgs e)
        {

        }
    }
}
