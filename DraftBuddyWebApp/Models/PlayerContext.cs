namespace DraftBuddyWebApp.Models
{
    using System.Data.Entity;

    public partial class PlayerContext : DbContext
    {
        public PlayerContext()
            : base("PlayerContext")
        {
        }

        public DbSet<FantasyStat2017> FantasyStat2017Entries { get; set; }
 
    }
}
