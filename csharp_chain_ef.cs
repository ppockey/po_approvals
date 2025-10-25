 // ITTPortal.Core.Abstractions/IPoApprovalAuditRepository.cs
using System.Threading;
using System.Threading.Tasks;

namespace ITTPortal.Core.Abstractions
{
    public interface IPoApprovalAuditRepository
    {
        Task InsertAsync(string poNumber, char oldStatus, char newStatus, string changedBy,
                         string? note, int? sequence, string? roleCode, char? category,
                         CancellationToken ct);
    }
}


// ITTPortal.Core.Abstractions/IPoApprovalChainRepository.cs
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ITTPortal.Core.Abstractions
{
    public interface IPoApprovalChainRepository
    {
        Task<bool> ChainExistsAsync(string poNumber, CancellationToken ct);
        Task CreateChainAsync(string poNumber, CancellationToken ct);
        Task InsertStagesAsync(string poNumber, IEnumerable<(int Seq, string RoleCode)> stages, CancellationToken ct);

        Task<(int Seq, string RoleCode)?> GetFirstPendingStageAsync(string poNumber, CancellationToken ct);
        Task SetStageStatusAsync(string poNumber, int sequence, char newStatus, CancellationToken ct);
        Task<bool> AllStagesApprovedAsync(string poNumber, CancellationToken ct);
        Task FinalizeChainAsync(string poNumber, char finalStatus, CancellationToken ct);
    }
}

// ITTPortal.Core.Abstractions/IPoApprovalOutboxRepository.cs
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ITTPortal.POApprovals.Models;

namespace ITTPortal.Core.Abstractions
{
    public interface IPoApprovalOutboxRepository
    {
        Task<IReadOnlyList<OutboxEventRow>> GetUnprocessedNewWaitingAsync(int top, CancellationToken ct);
        Task MarkProcessedAsync(long outboxId, CancellationToken ct);
        Task IncrementAttemptsAsync(long outboxId, CancellationToken ct);
    }
}

// entities
// ITTPortal.Core.Entities.POApprovals/PoApprovalChain.cs
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace ITTPortal.Core.Entities.POApprovals
{
    [Table("PO_ApprovalChain", Schema = "dbo")]
    public class PoApprovalChain
    {
        [Key]
        [MaxLength(20)]
        public string PoNumber { get; set; } = null!;

        public DateTime CreatedAtUtc { get; set; }
        public char Status { get; set; } // 'P','A','D'
        public DateTime? FinalizedAtUtc { get; set; }
    }
}


// ITTPortal.Core.Entities.POApprovals/PoApprovalStage.cs
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace ITTPortal.Core.Entities.POApprovals
{
    [Table("PO_ApprovalStage", Schema = "dbo")]
    public class PoApprovalStage
    {
        [MaxLength(20)]
        public string PoNumber { get; set; } = null!;
        public int Sequence { get; set; } // PK part

        [MaxLength(40)]
        public string RoleCode { get; set; } = null!;

        [MaxLength(100)]
        public string? ApproverUserId { get; set; }

        public char? Category { get; set; } // 'I'/'D'/null

        [Column(TypeName = "decimal(18,2)")]
        public decimal? ThresholdFrom { get; set; }

        [Column(TypeName = "decimal(18,2)")]
        public decimal? ThresholdTo { get; set; }

        public char Status { get; set; } // 'P','A','D','S'
        public DateTime? DecidedAtUtc { get; set; }
    }
}

// ITTPortal.Core.Entities.POApprovals/PoApprovalOutbox.cs
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace ITTPortal.Core.Entities.POApprovals
{
    [Table("PO_ApprovalOutbox", Schema = "dbo")]
    public class PoApprovalOutbox
    {
        [Key]
        public long OutboxId { get; set; }

        [MaxLength(40)]
        public string EventType { get; set; } = null!;

        [MaxLength(20)]
        public string PoNumber { get; set; } = null!;

        public DateTime OccurredAtUtc { get; set; }
        public string? PayloadJson { get; set; }
        public int Attempts { get; set; }
        public DateTime? ProcessedAtUtc { get; set; }

        [Column(TypeName = "decimal(18,2)")]
        public decimal? DirectAmount { get; set; }

        [Column(TypeName = "decimal(18,2)")]
        public decimal? IndirectAmount { get; set; }
    }
}

// ITTPortal.Core.Entities.POApprovals/PoApprovalAudit.cs
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace ITTPortal.Core.Entities.POApprovals
{
    [Table("PO_Approval_Audit", Schema = "dbo")]
    public class PoApprovalAudit
    {
        [Key]
        public long AuditId { get; set; }

        [MaxLength(20)]
        public string PoNumber { get; set; } = null!;

        public char OldStatus { get; set; }
        public char NewStatus { get; set; }

        [MaxLength(100)]
        public string ChangedBy { get; set; } = null!;

        public DateTime ChangedAtUtc { get; set; }
        public string? DecisionNote { get; set; }

        public int? Sequence { get; set; }

        [MaxLength(40)]
        public string? RoleCode { get; set; }

        public char? Category { get; set; }
    }
}

// configurations
// ITTPortal.Infrastructure.Configurations/POApprovalsApprovalChainConfiguration.cs
using ITTPortal.Core.Entities.POApprovals;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ITTPortal.Infrastructure.Configurations
{
    public sealed class POApprovalsApprovalChainConfiguration : IEntityTypeConfiguration<PoApprovalChain>
    {
        public void Configure(EntityTypeBuilder<PoApprovalChain> b)
        {
            b.ToTable("PO_ApprovalChain", "dbo");
            b.HasKey(x => x.PoNumber);
            b.Property(x => x.PoNumber).HasMaxLength(20).IsRequired();

            b.Property(x => x.Status).HasDefaultValue('P');
            b.Property(x => x.CreatedAtUtc).HasDefaultValueSql("sysutcdatetime()");
        }
    }
}

// ITTPortal.Infrastructure.Configurations/POApprovalsApprovalStageConfiguration.cs
using ITTPortal.Core.Entities.POApprovals;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ITTPortal.Infrastructure.Configurations
{
    public sealed class POApprovalsApprovalStageConfiguration : IEntityTypeConfiguration<PoApprovalStage>
    {
        public void Configure(EntityTypeBuilder<PoApprovalStage> b)
        {
            b.ToTable("PO_ApprovalStage", "dbo");
            b.HasKey(x => new { x.PoNumber, x.Sequence });

            b.Property(x => x.PoNumber).HasMaxLength(20).IsRequired();
            b.Property(x => x.RoleCode).HasMaxLength(40).IsRequired();
            b.Property(x => x.Status).HasDefaultValue('P');

            b.HasIndex(x => new { x.PoNumber, x.Status, x.Sequence })
             .HasDatabaseName("IX_PO_ApprovalStage_Po_Status_Seq");
        }
    }
}

// ITTPortal.Infrastructure.Configurations/POApprovalsApprovalOutboxConfiguration.cs
using ITTPortal.Core.Entities.POApprovals;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ITTPortal.Infrastructure.Configurations
{
    public sealed class POApprovalsApprovalOutboxConfiguration : IEntityTypeConfiguration<PoApprovalOutbox>
    {
        public void Configure(EntityTypeBuilder<PoApprovalOutbox> b)
        {
            b.ToTable("PO_ApprovalOutbox", "dbo");
            b.HasKey(x => x.OutboxId);

            b.Property(x => x.EventType).HasMaxLength(40).IsRequired();
            b.Property(x => x.PoNumber).HasMaxLength(20).IsRequired();
            b.Property(x => x.OccurredAtUtc).HasDefaultValueSql("sysutcdatetime()");
            b.Property(x => x.Attempts).HasDefaultValue(0);

            // Matches your filtered unique index for unprocessed events:
            b.HasIndex(x => new { x.EventType, x.PoNumber })
             .HasDatabaseName("UX_PO_ApprovalOutbox_Unprocessed")
             .IsUnique()
             .HasFilter("[ProcessedAtUtc] IS NULL");

            b.HasIndex(x => new { x.ProcessedAtUtc, x.Attempts })
             .HasDatabaseName("IX_PO_ApprovalOutbox_Queued");
        }
    }
}

// ITTPortal.Infrastructure.Configurations/POApprovalsApprovalAuditConfiguration.cs
using ITTPortal.Core.Entities.POApprovals;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ITTPortal.Infrastructure.Configurations
{
    public sealed class POApprovalsApprovalAuditConfiguration : IEntityTypeConfiguration<PoApprovalAudit>
    {
        public void Configure(EntityTypeBuilder<PoApprovalAudit> b)
        {
            b.ToTable("PO_Approval_Audit", "dbo");
            b.HasKey(x => x.AuditId);

            b.Property(x => x.PoNumber).HasMaxLength(20).IsRequired();
            b.Property(x => x.ChangedBy).HasMaxLength(100).IsRequired();
            b.Property(x => x.ChangedAtUtc).HasDefaultValueSql("sysutcdatetime()");

            b.HasIndex(x => new { x.PoNumber, x.Sequence, x.RoleCode, x.ChangedAtUtc })
             .HasDatabaseName("IX_PO_Approval_Audit_PO_Stage");
        }
    }
}

// dbsets
// In PortalDbContext
public DbSet<Core.Entities.POApprovals.PoApprovalChain> PoApprovalChains { get; set; }
public DbSet<Core.Entities.POApprovals.PoApprovalStage> PoApprovalStages { get; set; }
public DbSet<Core.Entities.POApprovals.PoApprovalOutbox> PoApprovalOutboxes { get; set; }
public DbSet<Core.Entities.POApprovals.PoApprovalAudit> PoApprovalAudits { get; set; }

// onmodelcreated
builder.ApplyConfiguration(new POApprovalsApprovalChainConfiguration());
builder.ApplyConfiguration(new POApprovalsApprovalStageConfiguration());
builder.ApplyConfiguration(new POApprovalsApprovalOutboxConfiguration());
builder.ApplyConfiguration(new POApprovalsApprovalAuditConfiguration());

// repositories
// ITTPortal.Infrastructure.Repositories/PoApprovalOutboxRepository.cs
using ITT.Logger.Abstractions;
using ITTPortal.Core.Abstractions;
using ITTPortal.Core.Entities.POApprovals;
using ITTPortal.Infrastructure;
using ITTPortal.POApprovals.Models;
using Microsoft.EntityFrameworkCore;

namespace ITTPortal.Infrastructure.Repositories
{
    public sealed class PoApprovalOutboxRepository : IPoApprovalOutboxRepository
    {
        private readonly PortalDbContext _db;
        private readonly ILoggerService _log;

        public PoApprovalOutboxRepository(PortalDbContext db, ILoggerService log)
            => (_db, _log) = (db, log);

        public async Task<IReadOnlyList<OutboxEventRow>> GetUnprocessedNewWaitingAsync(int top, CancellationToken ct)
        {
            var rows = await _db.Set<PoApprovalOutbox>()
                .AsNoTracking()
                .Where(x => x.ProcessedAtUtc == null && x.EventType == "PO_NEW_WAITING")
                .OrderBy(x => x.OutboxId)
                .Take(top)
                .Select(x => new OutboxEventRow(
                    x.OutboxId,
                    x.PoNumber,
                    x.EventType,
                    x.OccurredAtUtc,
                    x.DirectAmount,
                    x.IndirectAmount))
                .ToListAsync(ct);

            return rows;
        }

        public async Task MarkProcessedAsync(long outboxId, CancellationToken ct)
        {
            // Update without fetching full entity
            await _db.Set<PoApprovalOutbox>()
                .Where(x => x.OutboxId == outboxId)
                .ExecuteUpdateAsync(s => s
                    .SetProperty(x => x.ProcessedAtUtc, _ => DateTime.UtcNow),
                    ct);
        }

        public async Task IncrementAttemptsAsync(long outboxId, CancellationToken ct)
        {
            await _db.Set<PoApprovalOutbox>()
                .Where(x => x.OutboxId == outboxId)
                .ExecuteUpdateAsync(s => s
                    .SetProperty(x => x.Attempts, x => x.Attempts + 1),
                    ct);
        }
    }
}

// ITTPortal.Infrastructure.Repositories/PoApprovalChainRepository.cs
using ITTPortal.Core.Abstractions;
using ITTPortal.Core.Entities.POApprovals;
using ITTPortal.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace ITTPortal.Infrastructure.Repositories
{
    public sealed class PoApprovalChainRepository : IPoApprovalChainRepository
    {
        private readonly PortalDbContext _db;
        public PoApprovalChainRepository(PortalDbContext db) => _db = db;

        public Task<bool> ChainExistsAsync(string poNumber, CancellationToken ct)
            => _db.Set<PoApprovalChain>().AnyAsync(x => x.PoNumber == poNumber, ct);

        public async Task CreateChainAsync(string poNumber, CancellationToken ct)
        {
            _db.Set<PoApprovalChain>().Add(new PoApprovalChain
            {
                PoNumber = poNumber,
                Status = 'P',
                CreatedAtUtc = DateTime.UtcNow
            });
            await _db.SaveChangesAsync(ct);
        }

        public async Task InsertStagesAsync(string poNumber, IEnumerable<(int Seq, string RoleCode)> stages, CancellationToken ct)
        {
            if (stages == null) return;

            var entities = stages.Select(s => new PoApprovalStage
            {
                PoNumber = poNumber,
                Sequence = s.Seq,
                RoleCode = s.RoleCode,
                Status = 'P'
            }).ToList();

            _db.Set<PoApprovalStage>().AddRange(entities);
            await _db.SaveChangesAsync(ct);
        }

        public async Task<(int Seq, string RoleCode)?> GetFirstPendingStageAsync(string poNumber, CancellationToken ct)
        {
            var s = await _db.Set<PoApprovalStage>()
                .AsNoTracking()
                .Where(x => x.PoNumber == poNumber && x.Status == 'P')
                .OrderBy(x => x.Sequence)
                .Select(x => new { x.Sequence, x.RoleCode })
                .FirstOrDefaultAsync(ct);

            return s is null ? null : (s.Sequence, s.RoleCode);
        }

        public async Task SetStageStatusAsync(string poNumber, int sequence, char newStatus, CancellationToken ct)
        {
            // Update only if currently 'P'
            await _db.Set<PoApprovalStage>()
                .Where(x => x.PoNumber == poNumber && x.Sequence == sequence && x.Status == 'P')
                .ExecuteUpdateAsync(s => s
                    .SetProperty(x => x.Status, _ => newStatus)
                    .SetProperty(x => x.DecidedAtUtc, _ => (newStatus is 'A' or 'D' or 'S') ? DateTime.UtcNow : (DateTime?)null),
                    ct);
        }

        public async Task<bool> AllStagesApprovedAsync(string poNumber, CancellationToken ct)
        {
            var anyPending = await _db.Set<PoApprovalStage>()
                .AnyAsync(x => x.PoNumber == poNumber && x.Status == 'P', ct);

            return !anyPending;
        }

        public async Task FinalizeChainAsync(string poNumber, char finalStatus, CancellationToken ct)
        {
            await _db.Set<PoApprovalChain>()
                .Where(x => x.PoNumber == poNumber)
                .ExecuteUpdateAsync(s => s
                    .SetProperty(x => x.Status, _ => finalStatus)
                    .SetProperty(x => x.FinalizedAtUtc, _ => DateTime.UtcNow),
                    ct);
        }
    }
}

// ITTPortal.Infrastructure.Repositories/PoApprovalAuditRepository.cs
using ITTPortal.Core.Abstractions;
using ITTPortal.Core.Entities.POApprovals;
using ITTPortal.Infrastructure;

namespace ITTPortal.Infrastructure.Repositories
{
    public sealed class PoApprovalAuditRepository : IPoApprovalAuditRepository
    {
        private readonly PortalDbContext _db;
        public PoApprovalAuditRepository(PortalDbContext db) => _db = db;

        public async Task InsertAsync(string poNumber, char oldStatus, char newStatus, string changedBy,
                                      string? note, int? sequence, string? roleCode, char? category,
                                      CancellationToken ct)
        {
            _db.Set<PoApprovalAudit>().Add(new PoApprovalAudit
            {
                PoNumber = poNumber,
                OldStatus = oldStatus,
                NewStatus = newStatus,
                ChangedBy = changedBy,
                ChangedAtUtc = DateTime.UtcNow,
                DecisionNote = note,
                Sequence = sequence,
                RoleCode = roleCode,
                Category = category
            });

            await _db.SaveChangesAsync(ct);
        }
    }
}

// service updates
// ITTPortal.POApprovals.Services/ProcessApprovalOutboxJob.cs
using ITT.Logger.Abstractions;
using ITTPortal.Core.Abstractions;
using ITTPortal.Infrastructure;
using ITTPortal.POApprovals.Abstraction;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;

namespace ITTPortal.POApprovals.Services
{
    public sealed class ProcessApprovalOutboxJob : IProcessApprovalOutboxJob
    {
        private const string app = "ITTPortal.POApprovals.Services.ProcessApprovalOutboxJob";

        private readonly IPoApprovalOutboxRepository _outbox;
        private readonly IPoApprovalChainRepository _chainRepo;
        private readonly IPoApprovalChainBuilder _builder;
        private readonly IPoApprovalAuditRepository _audit;
        private readonly IPoApprovalNotifier _notifier;
        private readonly ILoggerService _log;
        private readonly PortalDbContext _db;

        public ProcessApprovalOutboxJob(
            IPoApprovalOutboxRepository outbox,
            IPoApprovalChainRepository chainRepo,
            IPoApprovalChainBuilder builder,
            IPoApprovalAuditRepository audit,
            IPoApprovalNotifier notifier,
            PortalDbContext db,
            ILoggerService log)
        {
            _outbox = outbox;
            _chainRepo = chainRepo;
            _builder = builder;
            _audit = audit;
            _notifier = notifier;
            _db = db;
            _log = log;
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var runId = Guid.NewGuid().ToString("N");
            var sw = Stopwatch.StartNew();
            _log.Info($"{app} [{runId}] Start …");

            var events = await _outbox.GetUnprocessedNewWaitingAsync(top: 50, ct);
            if (events.Count == 0)
            {
                _log.Info($"{app} [{runId}] No unprocessed events.");
                return;
            }

            foreach (var ev in events)
            {
                if (ct.IsCancellationRequested) ct.ThrowIfCancellationRequested();

                await using var tx = await _db.Database.BeginTransactionAsync(ct);
                try
                {
                    // 1) Idempotent chain create
                    if (!await _chainRepo.ChainExistsAsync(ev.PoNumber, ct))
                    {
                        await _chainRepo.CreateChainAsync(ev.PoNumber, ct);

                        var stages = await _builder.BuildAsync(ev.PoNumber, ev.DirectAmount, ev.IndirectAmount, ct);
                        if (stages.Count > 0)
                            await _chainRepo.InsertStagesAsync(ev.PoNumber, stages, ct);

                        await _audit.InsertAsync(ev.PoNumber, oldStatus: ' ', newStatus: 'P', changedBy: "system",
                                                 note: "Chain initialized", sequence: null, roleCode: null, category: null, ct);
                    }

                    // 2) Notify first pending
                    var first = await _chainRepo.GetFirstPendingStageAsync(ev.PoNumber, ct);
                    if (first is { } s)
                        await _notifier.NotifyStageReadyAsync(ev.PoNumber, s.Seq, s.RoleCode, ct);

                    // 3) Mark event processed
                    await _outbox.MarkProcessedAsync(ev.OutboxId, ct);

                    await tx.CommitAsync(ct);
                    _log.Info($"{app} [{runId}] PO {ev.PoNumber}: processed outbox {ev.OutboxId}.");
                }
                catch (Exception ex)
                {
                    await tx.RollbackAsync(ct);
                    _log.Error($"{app} [{runId}] PO {ev.PoNumber}: failed; Attempts++.", exception: ex);
                    await _outbox.IncrementAttemptsAsync(ev.OutboxId, ct);
                }
            }

            sw.Stop();
            _log.Info($"{app} [{runId}] Done in {sw.ElapsedMilliseconds} ms. Events: {events.Count}.");
        }
    }
}

// po approvals service - controller uses this
// ITTPortal.POApprovals.Services/PoApprovalsService.cs
using ITT.Logger.Abstractions;
using ITTPortal.Core.Abstractions;
using ITTPortal.Infrastructure;
using ITTPortal.POApprovals.Abstraction;
using Microsoft.EntityFrameworkCore;

namespace ITTPortal.POApprovals.Services
{
    public sealed class PoApprovalsService
    {
        private readonly IPoApprovalChainRepository _repo;
        private readonly IPoApprovalAuditRepository _audit;
        private readonly IPoApprovalNotifier _notifier;
        private readonly ILoggerService _log;
        private readonly PortalDbContext _db;

        public PoApprovalsService(IPoApprovalChainRepository repo,
                                  IPoApprovalAuditRepository audit,
                                  IPoApprovalNotifier notifier,
                                  PortalDbContext db,
                                  ILoggerService log)
        {
            _repo = repo; _audit = audit; _notifier = notifier; _db = db; _log = log;
        }

        public async Task ApproveAsync(string poNumber, int sequence, string userId, string? note, CancellationToken ct)
        {
            await using var tx = await _db.Database.BeginTransactionAsync(ct);
            try
            {
                await _repo.SetStageStatusAsync(poNumber, sequence, 'A', ct);
                await _audit.InsertAsync(poNumber, 'P', 'A', userId, note, sequence, roleCode: null, category: null, ct);

                var allApproved = await _repo.AllStagesApprovedAsync(poNumber, ct);
                if (allApproved)
                {
                    await _repo.FinalizeChainAsync(poNumber, 'A', ct);
                    await _audit.InsertAsync(poNumber, 'P', 'A', "system", "Chain finalized", null, null, null, ct);
                }
                else
                {
                    var next = await _repo.GetFirstPendingStageAsync(poNumber, ct);
                    if (next is { } s)
                        await _notifier.NotifyStageReadyAsync(poNumber, s.Seq, s.RoleCode, ct);
                }

                await tx.CommitAsync(ct);
            }
            catch
            {
                await tx.RollbackAsync(ct);
                throw;
            }
        }

        public async Task DenyAsync(string poNumber, int sequence, string userId, string? note, CancellationToken ct)
        {
            await using var tx = await _db.Database.BeginTransactionAsync(ct);
            try
            {
                await _repo.SetStageStatusAsync(poNumber, sequence, 'D', ct);
                await _audit.InsertAsync(poNumber, 'P', 'D', userId, note, sequence, roleCode: null, category: null, ct);

                await _repo.FinalizeChainAsync(poNumber, 'D', ct);
                await _audit.InsertAsync(poNumber, 'P', 'D', "system", "Chain finalized (denied)", null, null, null, ct);

                await tx.CommitAsync(ct);
            }
            catch
            {
                await tx.RollbackAsync(ct);
                throw;
            }
        }
    }
}

// PoDecisionService (PRMS write-back and PO_Decide) can stay as is; it already uses EF (ExecuteSqlInterpolatedAsync) and doesn’t depend on the three repos.







