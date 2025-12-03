using ITTPortal.Core.Entities.POApprovals;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ITTPortal.Infrastructure.Configurations
{
    public sealed class PoApprovalsPoHeaderConfiguration : IEntityTypeConfiguration<PoHeader>
    {
        public void Configure(EntityTypeBuilder<PoHeader> b)
        {
            // Table
            b.ToTable("PO_Header", "dbo");

            b.HasKey(x => x.PoHeaderId);

            b.HasIndex(x => x.PoNumber).IsUnique(); // matches UQ in your SQL script

            b.Property(x => x.PoNumber)
              .HasMaxLength(20)
              .IsRequired();

            b.Property(x => x.Status)
              .HasColumnType("char(1)")
              .IsRequired();

            b.Property(x => x.IsActive)
              .IsRequired();

        }
    }
}
