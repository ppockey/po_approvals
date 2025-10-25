// ==========================
// ITTPortal.POApprovals.Abstraction
// ==========================
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using ITTPortal.POApprovals.Models;

namespace ITTPortal.POApprovals.Abstraction
{
    public interface IProcessApprovalOutboxJob
    {
        Task RunAsync(CancellationToken ct);
    }

    public interface IApprovalChainBuilder
    {
        Task<IReadOnlyList<(int Seq, string RoleCode)>> BuildAsync(
            string poNumber, decimal? directAmount, decimal? indirectAmount, CancellationToken ct);
    }

    public interface IApprovalOutboxRepository
    {
        Task<IReadOnlyList<OutboxEventRow>> GetUnprocessedNewWaitingAsync(int top, CancellationToken ct);
        Task MarkProcessedAsync(long outboxId, SqlConnection cn, SqlTransaction tx, CancellationToken ct);
        Task IncrementAttemptsAsync(long outboxId, CancellationToken ct);
    }

    public interface IApprovalChainRepository
    {
        Task<bool> ChainExistsAsync(string poNumber, SqlConnection cn, SqlTransaction tx, CancellationToken ct);
        Task CreateChainAsync(string poNumber, SqlConnection cn, SqlTransaction tx, CancellationToken ct);
        Task InsertStagesAsync(string poNumber, IEnumerable<(int Seq, string RoleCode)> stages,
                               SqlConnection cn, SqlTransaction tx, CancellationToken ct);

        // Tx-aware overload (to see uncommitted work inside the same transaction)
        Task<(int Seq, string RoleCode)?> GetFirstPendingStageAsync(string poNumber, SqlConnection cn, SqlTransaction tx, CancellationToken ct);

        // Optional non-tx version (useful for read-only scenarios)
        Task<(int Seq, string RoleCode)?> GetFirstPendingStageAsync(string poNumber, CancellationToken ct);

        Task SetStageStatusAsync(string poNumber, int sequence, char newStatus,
                                 SqlConnection cn, SqlTransaction tx, CancellationToken ct);
        Task<bool> AllStagesApprovedAsync(string poNumber, SqlConnection cn, SqlTransaction tx, CancellationToken ct);
        Task FinalizeChainAsync(string poNumber, char finalStatus, SqlConnection cn, SqlTransaction tx, CancellationToken ct);
    }

    public interface IApprovalAuditRepository
    {
        Task InsertAsync(string poNumber, char oldStatus, char newStatus, string changedBy,
                         string? note, int? sequence, string? roleCode, char? category,
                         SqlConnection cn, SqlTransaction tx, CancellationToken ct);
    }

    public interface IApprovalNotifier
    {
        Task NotifyStageReadyAsync(string poNumber, int sequence, string roleCode, CancellationToken ct);
    }
}

// ==========================
// ITTPortal.POApprovals.Models
// ==========================
using System;

namespace ITTPortal.POApprovals.Models
{
    public sealed record OutboxEventRow(
        long OutboxId,
        string PoNumber,
        string EventType,
        DateTime OccurredAtUtc,
        decimal? DirectAmount,
        decimal? IndirectAmount
    );
}

// ==========================
// ITTPortal.POApprovals.Infrastructure - Chain Builder
// ==========================
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ITT.Logger.Abstractions;
using ITTPortal.POApprovals.Abstraction;

namespace ITTPortal.POApprovals.Infrastructure
{
    public sealed class ApprovalChainBuilder : IApprovalChainBuilder
    {
        private readonly ILoggerService _log;
        public ApprovalChainBuilder(ILoggerService log) => _log = log;

        public Task<IReadOnlyList<(int Seq, string RoleCode)>> BuildAsync(
            string poNumber, decimal? directAmount, decimal? indirectAmount, CancellationToken ct)
        {
            var roles = new List<string>();

            // Indirect rules (manager notes)
            if (indirectAmount is decimal i)
            {
                if (i <= 2000m) roles.AddRange(new[] { "LPM", "GM", "SFC" });
                else            roles.AddRange(new[] { "LPM", "GM", "SFC", "VP" });
            }

            // Direct rules
            if (directAmount is decimal d)
            {
                if (d > 100_000m) roles.AddRange(new[] { "LPM", "SFC", "GM" });
                else if (d > 50_000m) roles.Add("LPM");
            }

            // Stable de-duplication in order of appearance
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var ordered = roles.Where(r => seen.Add(r)).ToList();

            var result = ordered.Select((r, idx) => (idx + 1, r)).ToList().AsReadOnly();
            _log.Info($"ApprovalChainBuilder: PO {poNumber} -> {result.Count} stage(s).");
            return Task.FromResult<IReadOnlyList<(int, string)>>(result);
        }
    }
}

// ==========================
// ITTPortal.POApprovals.Infrastructure - Repositories
// ==========================
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using ITT.Logger.Abstractions;
using ITTPortal.POApprovals.Abstraction;
using ITTPortal.POApprovals.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace ITTPortal.POApprovals.Infrastructure
{
    public sealed class ApprovalOutboxRepository : IApprovalOutboxRepository
    {
        private readonly string _conn;
        private readonly ILoggerService _log;

        public ApprovalOutboxRepository(IConfiguration cfg, ILoggerService log)
        {
            _conn = cfg.GetConnectionString("WebappsDb")
                ?? cfg.GetConnectionString("DefaultConnection")
                ?? throw new InvalidOperationException("Missing connection string WebappsDb/DefaultConnection.");
            _log = log;
        }

        public async Task<IReadOnlyList<OutboxEventRow>> GetUnprocessedNewWaitingAsync(int top, CancellationToken ct)
        {
            const string SQL = @"
                SELECT TOP (@top)
                       OutboxId, PoNumber, EventType, OccurredAtUtc, DirectAmount, IndirectAmount
                FROM dbo.PO_ApprovalOutbox WITH (READPAST)
                WHERE ProcessedAtUtc IS NULL AND EventType = 'PO_NEW_WAITING'
                ORDER BY OutboxId";

            using var cn = new SqlConnection(_conn);
            await cn.OpenAsync(ct);
            using var cmd = new SqlCommand(SQL, cn);
            cmd.Parameters.Add(new SqlParameter("@top", SqlDbType.Int) { Value = top });

            var list = new List<OutboxEventRow>(top);
            using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                list.Add(new OutboxEventRow(
                    rdr.GetInt64(0),
                    rdr.GetString(1),
                    rdr.GetString(2),
                    rdr.GetDateTime(3),
                    rdr.IsDBNull(4) ? (decimal?)null : rdr.GetDecimal(4),
                    rdr.IsDBNull(5) ? (decimal?)null : rdr.GetDecimal(5)
                ));
            }
            return list;
        }

        public async Task MarkProcessedAsync(long outboxId, SqlConnection cn, SqlTransaction tx, CancellationToken ct)
        {
            const string SQL = @"UPDATE dbo.PO_ApprovalOutbox
                                 SET ProcessedAtUtc = SYSUTCDATETIME()
                                 WHERE OutboxId = @id";
            using var cmd = new SqlCommand(SQL, cn, tx);
            cmd.Parameters.Add(new SqlParameter("@id", SqlDbType.BigInt) { Value = outboxId });
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task IncrementAttemptsAsync(long outboxId, CancellationToken ct)
        {
            const string SQL = @"UPDATE dbo.PO_ApprovalOutbox
                                 SET Attempts = Attempts + 1
                                 WHERE OutboxId = @id";
            using var cn = new SqlConnection(_conn);
            await cn.OpenAsync(ct);
            using var cmd = new SqlCommand(SQL, cn);
            cmd.Parameters.Add(new SqlParameter("@id", SqlDbType.BigInt) { Value = outboxId });
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    public sealed class ApprovalChainRepository : IApprovalChainRepository
    {
        private readonly string _conn;
        public ApprovalChainRepository(IConfiguration cfg)
        {
            _conn = cfg.GetConnectionString("WebappsDb")
                ?? cfg.GetConnectionString("DefaultConnection")
                ?? throw new InvalidOperationException("Missing connection string WebappsDb/DefaultConnection.");
        }

        public async Task<bool> ChainExistsAsync(string poNumber, SqlConnection cn, SqlTransaction tx, CancellationToken ct)
        {
            const string SQL = "SELECT 1 FROM dbo.PO_ApprovalChain WHERE PoNumber=@po";
            using var cmd = new SqlCommand(SQL, cn, tx);
            cmd.Parameters.Add(new SqlParameter("@po", SqlDbType.NVarChar, 20) { Value = poNumber });
            var r = await cmd.ExecuteScalarAsync(ct);
            return r != null;
        }

        public async Task CreateChainAsync(string poNumber, SqlConnection cn, SqlTransaction tx, CancellationToken ct)
        {
            const string SQL = @"INSERT INTO dbo.PO_ApprovalChain(PoNumber) VALUES(@po)";
            using var cmd = new SqlCommand(SQL, cn, tx);
            cmd.Parameters.Add(new SqlParameter("@po", SqlDbType.NVarChar, 20) { Value = poNumber });
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task InsertStagesAsync(string poNumber, IEnumerable<(int Seq, string RoleCode)> stages,
                                            SqlConnection cn, SqlTransaction tx, CancellationToken ct)
        {
            const string SQL = @"
                INSERT INTO dbo.PO_ApprovalStage
                (PoNumber, Sequence, RoleCode, Status)
                VALUES (@po, @seq, @role, 'P')";
            foreach (var (seq, role) in stages)
            {
                using var cmd = new SqlCommand(SQL, cn, tx);
                cmd.Parameters.Add(new SqlParameter("@po", SqlDbType.NVarChar, 20) { Value = poNumber });
                cmd.Parameters.Add(new SqlParameter("@seq", SqlDbType.Int) { Value = seq });
                cmd.Parameters.Add(new SqlParameter("@role", SqlDbType.NVarChar, 40) { Value = role });
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }

        public async Task<(int Seq, string RoleCode)?> GetFirstPendingStageAsync(string poNumber, SqlConnection cn, SqlTransaction tx, CancellationToken ct)
        {
            const string SQL = @"
                SELECT TOP (1) Sequence, RoleCode
                FROM dbo.PO_ApprovalStage
                WHERE PoNumber=@po AND Status='P'
                ORDER BY Sequence";
            using var cmd = new SqlCommand(SQL, cn, tx);
            cmd.Parameters.Add(new SqlParameter("@po", SqlDbType.NVarChar, 20) { Value = poNumber });
            using var rdr = await cmd.ExecuteReaderAsync(ct);
            if (await rdr.ReadAsync(ct))
                return (rdr.GetInt32(0), rdr.GetString(1));
            return null;
        }

        public async Task<(int Seq, string RoleCode)?> GetFirstPendingStageAsync(string poNumber, CancellationToken ct)
        {
            const string SQL = @"
                SELECT TOP (1) Sequence, RoleCode
                FROM dbo.PO_ApprovalStage
                WHERE PoNumber=@po AND Status='P'
                ORDER BY Sequence";
            using var cn = new SqlConnection(_conn);
            await cn.OpenAsync(ct);
            using var cmd = new SqlCommand(SQL, cn);
            cmd.Parameters.Add(new SqlParameter("@po", SqlDbType.NVarChar, 20) { Value = poNumber });
            using var rdr = await cmd.ExecuteReaderAsync(ct);
            if (await rdr.ReadAsync(ct))
                return (rdr.GetInt32(0), rdr.GetString(1));
            return null;
        }

        public async Task SetStageStatusAsync(string poNumber, int sequence, char newStatus,
                                              SqlConnection cn, SqlTransaction tx, CancellationToken ct)
        {
            const string SQL = @"
                UPDATE dbo.PO_ApprovalStage
                SET Status=@st, DecidedAtUtc = CASE WHEN @st IN ('A','D','S') THEN SYSUTCDATETIME() ELSE DecidedAtUtc END
                WHERE PoNumber=@po AND Sequence=@seq AND Status='P'";
            using var cmd = new SqlCommand(SQL, cn, tx);
            cmd.Parameters.Add(new SqlParameter("@po", SqlDbType.NVarChar, 20) { Value = poNumber });
            cmd.Parameters.Add(new SqlParameter("@seq", SqlDbType.Int) { Value = sequence });
            cmd.Parameters.Add(new SqlParameter("@st", SqlDbType.Char, 1) { Value = newStatus });
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task<bool> AllStagesApprovedAsync(string poNumber, SqlConnection cn, SqlTransaction tx, CancellationToken ct)
        {
            const string SQL = @"
                SELECT CASE WHEN EXISTS (
                    SELECT 1 FROM dbo.PO_ApprovalStage WHERE PoNumber=@po AND Status='P'
                ) THEN 0
                ELSE 1 END";
            using var cmd = new SqlCommand(SQL, cn, tx);
            cmd.Parameters.Add(new SqlParameter("@po", SqlDbType.NVarChar, 20) { Value = poNumber });
            var r = await cmd.ExecuteScalarAsync(ct);
            return Convert.ToInt32(r) == 1;
        }

        public async Task FinalizeChainAsync(string poNumber, char finalStatus, SqlConnection cn, SqlTransaction tx, CancellationToken ct)
        {
            const string SQL = @"
                UPDATE dbo.PO_ApprovalChain
                SET Status=@st, FinalizedAtUtc = SYSUTCDATETIME()
                WHERE PoNumber=@po";
            using var cmd = new SqlCommand(SQL, cn, tx);
            cmd.Parameters.Add(new SqlParameter("@po", SqlDbType.NVarChar, 20) { Value = poNumber });
            cmd.Parameters.Add(new SqlParameter("@st", SqlDbType.Char, 1) { Value = finalStatus });
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    public sealed class ApprovalAuditRepository : IApprovalAuditRepository
    {
        private readonly string _conn;
        public ApprovalAuditRepository(IConfiguration cfg)
        {
            _conn = cfg.GetConnectionString("WebappsDb")
                ?? cfg.GetConnectionString("DefaultConnection")
                ?? throw new InvalidOperationException("Missing connection string WebappsDb/DefaultConnection.");
        }

        public async Task InsertAsync(string poNumber, char oldStatus, char newStatus, string changedBy,
                                      string? note, int? sequence, string? roleCode, char? category,
                                      SqlConnection cn, SqlTransaction tx, CancellationToken ct)
        {
            const string SQL = @"
                INSERT dbo.PO_Approval_Audit
                  (PoNumber, OldStatus, NewStatus, ChangedBy, ChangedAtUtc, DecisionNote, Sequence, RoleCode, Category)
                VALUES
                  (@po, @old, @new, @by, SYSUTCDATETIME(), @note, @seq, @role, @cat)";
            using var cmd = new SqlCommand(SQL, cn, tx);
            cmd.Parameters.Add(new SqlParameter("@po",   SqlDbType.NVarChar, 20)  { Value = poNumber });
            cmd.Parameters.Add(new SqlParameter("@old",  SqlDbType.Char, 1)       { Value = oldStatus });
            cmd.Parameters.Add(new SqlParameter("@new",  SqlDbType.Char, 1)       { Value = newStatus });
            cmd.Parameters.Add(new SqlParameter("@by",   SqlDbType.NVarChar, 100) { Value = changedBy });
            cmd.Parameters.Add(new SqlParameter("@note", SqlDbType.NVarChar, 4000){ Value = (object?)note ?? DBNull.Value });
            cmd.Parameters.Add(new SqlParameter("@seq",  SqlDbType.Int)           { Value = (object?)sequence ?? DBNull.Value });
            cmd.Parameters.Add(new SqlParameter("@role", SqlDbType.NVarChar, 40)  { Value = (object?)roleCode ?? DBNull.Value });
            cmd.Parameters.Add(new SqlParameter("@cat",  SqlDbType.Char, 1)       { Value = (object?)category ?? DBNull.Value });
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}

// ==========================
// ITTPortal.POApprovals.Services - Periodic Job
// ==========================
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using ITT.Logger.Abstractions;
using ITTPortal.POApprovals.Abstraction;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace ITTPortal.POApprovals.Services
{
    /// <summary>
    /// Consumes PO_NEW_WAITING events, creates the approval chain, notifies first approver, marks processed.
    /// Safe to run periodically (cron).
    /// </summary>
    public sealed class ProcessApprovalOutboxJob : IProcessApprovalOutboxJob
    {
        private const string app = "ITTPortal.POApprovals.Services.ProcessApprovalOutboxJob";

        private readonly IApprovalOutboxRepository _outbox;
        private readonly IApprovalChainRepository _chainRepo;
        private readonly IApprovalChainBuilder _builder;
        private readonly IApprovalAuditRepository _audit;
        private readonly IApprovalNotifier _notifier; // stub ok
        private readonly ILoggerService _log;
        private readonly string _conn;

        public ProcessApprovalOutboxJob(
            IApprovalOutboxRepository outbox,
            IApprovalChainRepository chainRepo,
            IApprovalChainBuilder builder,
            IApprovalAuditRepository audit,
            IApprovalNotifier notifier,
            IConfiguration cfg,
            ILoggerService log)
        {
            _outbox = outbox;
            _chainRepo = chainRepo;
            _builder = builder;
            _audit = audit;
            _notifier = notifier;
            _log = log;
            _conn = cfg.GetConnectionString("WebappsDb")
                 ?? cfg.GetConnectionString("DefaultConnection")
                 ?? throw new InvalidOperationException("Missing connection string WebappsDb/DefaultConnection.");
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

                using var cn = new SqlConnection(_conn);
                await cn.OpenAsync(ct);
                var tx = await cn.BeginTransactionAsync(ct);

                try
                {
                    // 1) Idempotent chain create
                    if (!await _chainRepo.ChainExistsAsync(ev.PoNumber, cn, tx, ct))
                    {
                        await _chainRepo.CreateChainAsync(ev.PoNumber, cn, tx, ct);

                        var stages = await _builder.BuildAsync(ev.PoNumber, ev.DirectAmount, ev.IndirectAmount, ct);
                        if (stages.Count > 0)
                            await _chainRepo.InsertStagesAsync(ev.PoNumber, stages, cn, tx, ct);

                        // Optional: audit chain initialization
                        await _audit.InsertAsync(ev.PoNumber, oldStatus: ' ', newStatus: 'P', changedBy: "system",
                                                 note: "Chain initialized", sequence: null, roleCode: null, category: null,
                                                 cn, (SqlTransaction)tx, ct);
                    }

                    // 2) Notify first pending (tx-aware read)
                    var first = await _chainRepo.GetFirstPendingStageAsync(ev.PoNumber, cn, (SqlTransaction)tx, ct);
                    if (first is { } s)
                        await _notifier.NotifyStageReadyAsync(ev.PoNumber, s.Seq, s.RoleCode, ct);

                    // 3) Mark event processed
                    await _outbox.MarkProcessedAsync(ev.OutboxId, cn, (SqlTransaction)tx, ct);

                    await ((SqlTransaction)tx).CommitAsync(ct);
                    _log.Info($"{app} [{runId}] PO {ev.PoNumber}: processed outbox {ev.OutboxId}.");
                }
                catch (Exception ex)
                {
                    await ((SqlTransaction)tx).RollbackAsync(ct);
                    _log.Error($"{app} [{runId}] PO {ev.PoNumber}: failed; Attempts++.", exception: ex);
                    await _outbox.IncrementAttemptsAsync(ev.OutboxId, ct);
                }
            }

            sw.Stop();
            _log.Info($"{app} [{runId}] Done in {sw.ElapsedMilliseconds} ms. Events: {events.Count}.");
        }
    }
}

// ==========================
// ITTPortal.POApprovals.Services - Approvals Service (Approve/Deny)
// ==========================
using System;
using System.Threading;
using System.Threading.Tasks;
using ITT.Logger.Abstractions;
using ITTPortal.POApprovals.Abstraction;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace ITTPortal.POApprovals.Services
{
    public sealed class PoApprovalsService
    {
        private readonly IApprovalChainRepository _repo;
        private readonly IApprovalAuditRepository _audit;
        private readonly IApprovalNotifier _notifier;
        private readonly ILoggerService _log;
        private readonly string _conn;

        public PoApprovalsService(IApprovalChainRepository repo, IApprovalAuditRepository audit,
                                  IApprovalNotifier notifier, IConfiguration cfg, ILoggerService log)
        {
            _repo = repo; _audit = audit; _notifier = notifier; _log = log;
            _conn = cfg.GetConnectionString("WebappsDb")
                 ?? cfg.GetConnectionString("DefaultConnection")
                 ?? throw new InvalidOperationException("Missing connection string WebappsDb/DefaultConnection.");
        }

        public async Task ApproveAsync(string poNumber, int sequence, string userId, string? note, CancellationToken ct)
        {
            using var cn = new SqlConnection(_conn);
            await cn.OpenAsync(ct);
            var tx = await cn.BeginTransactionAsync(ct);

            try
            {
                await _repo.SetStageStatusAsync(poNumber, sequence, 'A', cn, (SqlTransaction)tx, ct);
                await _audit.InsertAsync(poNumber, 'P', 'A', userId, note, sequence, roleCode: null, category: null, cn, (SqlTransaction)tx, ct);

                var allApproved = await _repo.AllStagesApprovedAsync(poNumber, cn, (SqlTransaction)tx, ct);
                if (allApproved)
                {
                    await _repo.FinalizeChainAsync(poNumber, 'A', cn, (SqlTransaction)tx, ct);
                    await _audit.InsertAsync(poNumber, 'P', 'A', "system", "Chain finalized", null, null, null, cn, (SqlTransaction)tx, ct);
                }
                else
                {
                    var next = await _repo.GetFirstPendingStageAsync(poNumber, cn, (SqlTransaction)tx, ct); // tx-aware
                    if (next is { } s)
                        await _notifier.NotifyStageReadyAsync(poNumber, s.Seq, s.RoleCode, ct);
                }

                await ((SqlTransaction)tx).CommitAsync(ct);
            }
            catch
            {
                await ((SqlTransaction)tx).RollbackAsync(ct);
                throw;
            }
        }

        public async Task DenyAsync(string poNumber, int sequence, string userId, string? note, CancellationToken ct)
        {
            using var cn = new SqlConnection(_conn);
            await cn.OpenAsync(ct);
            var tx = await cn.BeginTransactionAsync(ct);

            try
            {
                await _repo.SetStageStatusAsync(poNumber, sequence, 'D', cn, (SqlTransaction)tx, ct);
                await _audit.InsertAsync(poNumber, 'P', 'D', userId, note, sequence, roleCode: null, category: null, cn, (SqlTransaction)tx, ct);

                await _repo.FinalizeChainAsync(poNumber, 'D', cn, (SqlTransaction)tx, ct);
                await _audit.InsertAsync(poNumber, 'P', 'D', "system", "Chain finalized (denied)", null, null, null, cn, (SqlTransaction)tx, ct);

                await ((SqlTransaction)tx).CommitAsync(ct);
            }
            catch
            {
                await ((SqlTransaction)tx).RollbackAsync(ct);
                throw;
            }
        }
    }
}

// ==========================
// ITTPortal.POApprovals.Infrastructure - Notifier Stub
// ==========================
using System.Threading;
using System.Threading.Tasks;
using ITTPortal.POApprovals.Abstraction;

namespace ITTPortal.POApprovals.Infrastructure
{
    public sealed class NoopApprovalNotifier : IApprovalNotifier
    {
        public Task NotifyStageReadyAsync(string poNumber, int sequence, string roleCode, CancellationToken ct)
            => Task.CompletedTask; // replace with email/queue later
    }
}

// ==========================
// DI Registration (example)
// ==========================
// in Startup/Program.cs
using ITTPortal.POApprovals.Abstraction;
using ITTPortal.POApprovals.Infrastructure;
using ITTPortal.POApprovals.Services;

services.AddScoped<IApprovalChainBuilder, ApprovalChainBuilder>();
services.AddScoped<IApprovalOutboxRepository, ApprovalOutboxRepository>();
services.AddScoped<IApprovalChainRepository, ApprovalChainRepository>();
services.AddScoped<IApprovalAuditRepository, ApprovalAuditRepository>();
services.AddScoped<IApprovalNotifier, NoopApprovalNotifier>();
services.AddScoped<IProcessApprovalOutboxJob, ProcessApprovalOutboxJob>();
services.AddScoped<PoApprovalsService>();

// Example cron host usage:
using (var scope = services.BuildServiceProvider().CreateScope())
{
    var job = scope.ServiceProvider.GetRequiredService<IProcessApprovalOutboxJob>();
    await job.RunAsync(CancellationToken.None);
}

// minimal controller example
/*
User-facing endpoints: PoApprovalsService.ApproveAsync(...), PoApprovalsService.DenyAsync(...)
Optional admin/ops endpoint: IProcessApprovalOutboxJob.RunAsync(...) (usually run by cron, but you can wire a manual trigger)
Summary: Controllers call PoApprovalsService for Approve/Deny; the cron job runs IProcessApprovalOutboxJob. You can expose the job via an admin endpoint if desired.
*/
using ITTPortal.POApprovals.Services;
using ITTPortal.POApprovals.Abstraction;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace ITTPortal.POApprovals.Api.Controllers
{
    [ApiController]
    [Route("api/po-approvals")]
    [Authorize] // adjust as needed
    public sealed class PoApprovalsController : ControllerBase
    {
        private readonly PoApprovalsService _svc;

        public PoApprovalsController(PoApprovalsService svc) => _svc = svc;

        public sealed record DecisionDto(string? Note);

        [HttpPost("{poNumber}/stages/{sequence:int}/approve")]
        public async Task<IActionResult> Approve(
            string poNumber, int sequence, [FromBody] DecisionDto dto, CancellationToken ct)
        {
            var userId = User.Identity?.Name ?? "unknown";
            await _svc.ApproveAsync(poNumber, sequence, userId, dto?.Note, ct);
            return NoContent();
        }

        [HttpPost("{poNumber}/stages/{sequence:int}/deny")]
        public async Task<IActionResult> Deny(
            string poNumber, int sequence, [FromBody] DecisionDto dto, CancellationToken ct)
        {
            var userId = User.Identity?.Name ?? "unknown";
            await _svc.DenyAsync(poNumber, sequence, userId, dto?.Note, ct);
            return NoContent();
        }
    }

    // Optional: manual trigger for the periodic job
    [ApiController]
    [Route("api/po-approvals/admin")]
    [Authorize(Roles = "ApprovalsAdmin")]
    public sealed class PoApprovalsAdminController : ControllerBase
    {
        private readonly IProcessApprovalOutboxJob _job;
        public PoApprovalsAdminController(IProcessApprovalOutboxJob job) => _job = job;

        [HttpPost("process-outbox")]
        public async Task<IActionResult> ProcessOutbox(CancellationToken ct)
        {
            await _job.RunAsync(ct);
            return Accepted(); // returns 202
        }
    }
}
