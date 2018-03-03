using CsvHelper;
using System.Data.Entity;
using System.Data.Entity.Migrations;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DraftBuddyWebApp.Models
{
    public class PlayerDatabaseInitializer : DropCreateDatabaseIfModelChanges<PlayerContext>
    {
        protected override void Seed(PlayerContext context)
        {
            Assembly assembly = Assembly.GetExecutingAssembly();
            string resourceName = "SeedData.testDbOutput.csv";
            using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            {
                using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
                {
                    CsvReader csvReader = new CsvReader(reader);
                    var players = csvReader.GetRecords<FantasyStat2017>().ToArray();
                    context.FantasyStat2017Entries.AddOrUpdate(c => c.Pid, players);
                }
            }
        }
    }
    
}