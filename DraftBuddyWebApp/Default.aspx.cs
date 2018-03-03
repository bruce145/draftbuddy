using System;
using System.Linq;
using System.Web.UI;

namespace DraftBuddyWebApp
{
    public partial class _Default : Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            
        }

        public IQueryable<Models.FantasyStat2017> GetPlayers()
        {
            System.Diagnostics.Debug.WriteLine("Loading players");
            var _db = new Models.PlayerContext();
            IQueryable<Models.FantasyStat2017> query = _db.FantasyStat2017Entries;
            
            query = from p in _db.FantasyStat2017Entries
                    orderby p.Goals descending
                    select p;
               
            return query;

        }
    }
}