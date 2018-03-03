namespace DraftBuddyWebApp.Models
{

    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    

    public partial class FantasyStat2017
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int Pid { get; set; }

        [StringLength(255)]
        public string LastName { get; set; }

        [StringLength(255)]
        public string FirstName { get; set; }

        public int? GP { get; set; }

        public int? Goals { get; set; }

        public int? Assists { get; set; }

        [Column("+-")]
        public int? PlusMinus { get; set; }

        public int? PIM { get; set; }

        public int? PPP { get; set; }

        public int? GWG { get; set; }

        public int? Shots { get; set; }

        public int? AvgTOI { get; set; }

        public int? WINS { get; set; }

        public int? GA { get; set; }

        public int? Saves { get; set; }
    }
}
