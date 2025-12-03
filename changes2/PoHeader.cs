using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace ITTPortal.Core.Entities.POApprovals
{
    /// <summary>
    /// Minimal write-model for dbo.PO_Header used to set Status ('W','A','D').
    /// </summary>
    [Table("PO_Header", Schema = "dbo")]
    public sealed class PoHeader
    {
        // PK
        [Key]
        public long PoHeaderId { get; set; }

        // Alternate key in DB (unique)
        [Required, MaxLength(20)]
        public string PoNumber { get; set; } = string.Empty;

        // 'W' | 'A' | 'D'
        [Required]
        public char Status { get; set; }

        // used by queries but not strictly required for updates
        public bool IsActive { get; set; }
    }
}
