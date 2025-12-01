using ITT.Logger.Abstractions;
using ITTPortal.Core.Abstractions;
using ITTPortal.Core.Entities.POApprovals;
using ITTPortal.Infrastructure;
using ITTPortal.POApprovals.Abstraction;
using ITTPortal.POApprovals.Models; // for Decision (Domain enum)
using Microsoft.EntityFrameworkCore;

namespace ITTPortal.POApprovals.Services
{
    /// <summary>
    /// Thrown when an approval action is attempted on a PO whose approval chain
    /// is not in a Pending ('P') state (e.g., already finalized as 'A' or 'D').
    /// Intended to be mapped to HTTP 409 by the API layer.
    /// </summary>
    public sealed class ChainNotPendingException : Exception
    {
        public string PoNumber { get; }
        public char CurrentStatus { get; }

        public ChainNotPendingException(string poNumber, char currentStatus)
            : base($"Approval chain is not pending for PO '{poNumber}' (current status '{currentStatus}').")
        {
            PoNumber = poNumber;
            CurrentStatus = currentStatus;
        }
    }

    /// <summary>
    /// Supplies PRMS-specific inputs required to write the approval audit row:
    ///  - amountType: 'D' (Direct Material) or 'I' (Indirect/Expense)
    ///  - bracket: numeric level threshold for the approving stage
    ///  - approver: PRMS approver identifier (e.g., 'SCHENCK')
    /// </summary>
    public interface IPrmsAuditInputResolver
    {
        /// <summary>
        /// Resolve PRMS audit inputs for a terminal approval event.
        /// Implementations typically inspect the current stage / DOA level to derive values.
        /// </summary>
        Task<(char amountType, decimal bracket, string approver)> ResolveAsync(
            string poNumber, int sequence, string userId, CancellationToken ct);
    }

    /// <summary>
    /// Safe default resolver that lets the service run immediately.
    /// - amountType defaults to 'D'
    /// - bracket defaults to 0
    /// - approver is derived from the userId (left of '@', uppercased)
    /// Replace with a concrete implementation that queries your DOA/Stage data.
    /// </summary>
    internal sealed class DefaultPrmsAuditInputResolver : IPrmsAuditInputResolver
    {
        public Task<(char amountType, decimal bracket, string approver)> ResolveAsync(
            string poNumber, int sequence, string userId, CancellationToken ct)
        {
            var approver = MapUserIdToPrmsApprover(userId);
            return Task.FromResult(('D', 0m, approver));
        }

        private static string MapUserIdToPrmsApprover(string userId)
        {
            if (string.IsNullOrWhiteSpace(userId)) return "SYSTEM";
            var at = userId.IndexOf('@');
            var core = at > 0 ? userId[..at] : userId;
            return core.Trim().ToUpperInvariant();
        }
    }

    /// <summary>
    /// Orchestrates local PO approval workflow and PRMS side-effects.
    /// 
    /// Rules:
    /// - Stage Approve/Deny updates occur locally in SQL Server.
    /// - If the entire chain becomes Approved:
    ///     1) finalize locally (Status 'A' in SQL),
    ///     2) write the PRMS approval audit workfile row (INPVP500) with P5STAT = 'A',
    ///     3) fire the PRMS trigger (INPTP500) so PRMS performs its own transition (to 'Y').
    /// - If a stage is Denied:
    ///     - finalize locally (Status 'D'),
    ///     - DO NOT write back to PRMS (by design).
    /// - All actions are audited to dbo.PO_Approval_Audit.
    /// - Guards attempts when chain is not Pending ('P') via ChainNotPendingException.
    /// </summary>
    public sealed class PoApprovalsService
    {
        private readonly IPoApprovalChainRepository _repo;
        private readonly IPoApprovalAuditRepository _audit;
        private readonly IPoApprovalNotifier _notifier;
        private readonly IPrmsWriter _prmsWriter;
        private readonly IPrmsAuditInputResolver _prmsAuditResolver;
        private readonly ILoggerService _log;
        private readonly PortalDbContext _db;

        /// <summary>
        /// Preferred constructor. Provide an <see cref="IPrmsAuditInputResolver"/> to derive
        /// PRMS audit inputs (amount type, bracket, approver) from your DOA/stage data.
        /// </summary>
        public PoApprovalsService(IPoApprovalChainRepository repo,
                                  IPoApprovalAuditRepository audit,
                                  IPoApprovalNotifier notifier,
                                  IPrmsWriter prmsWriter,
                                  IPrmsAuditInputResolver prmsAuditResolver,
                                  PortalDbContext db,
                                  ILoggerService log)
        {
            _repo = repo ?? throw new ArgumentNullException(nameof(repo));
            _audit = audit ?? throw new ArgumentNullException(nameof(audit));
            _notifier = notifier ?? throw new ArgumentNullException(nameof(notifier));
            _prmsWriter = prmsWriter ?? throw new ArgumentNullException(nameof(prmsWriter));
            _prmsAuditResolver = prmsAuditResolver ?? throw new ArgumentNullException(nameof(prmsAuditResolver));
            _db = db ?? throw new ArgumentNullException(nameof(db));
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        /// <summary>
        /// Backward-compatible constructor that uses a default resolver so you can drop this in immediately.
        /// Replace with the preferred constructor when you wire a real resolver.
        /// </summary>
        public PoApprovalsService(IPoApprovalChainRepository repo,
                                  IPoApprovalAuditRepository audit,
                                  IPoApprovalNotifier notifier,
                                  IPrmsWriter prmsWriter,
                                  PortalDbContext db,
                                  ILoggerService log)
            : this(repo, audit, notifier, prmsWriter, new DefaultPrmsAuditInputResolver(), db, log)
        { }

        /// <summary>
        /// Approves a single stage. If this action causes the whole chain to become approved,
        /// we finalize locally AND then write-back to PRMS via (INPVP500 + INPTP500).
        /// Throws <see cref="ChainNotPendingException"/> when the chain is not in 'P'.
        /// </summary>
        /// <param name="poNumber">PO identifier.</param>
        /// <param name="sequence">Stage sequence being approved.</param>
        /// <param name="userId">Acting user (typically an email or network id).</param>
        /// <param name="note">Optional audit note.</param>
        /// <param name="ct">Cancellation token.</param>
        public async Task ApproveAsync(string poNumber, int sequence, string userId, string? note, CancellationToken ct)
        {
            await using var tx = await _db.Database.BeginTransactionAsync(ct);

            try
            {
                // Guard & lock the chain row to prevent concurrent finalize attempts.
                _ = await LoadChainForUpdateAsync(poNumber, ct);

                // 1) Local stage decision + audit
                await _repo.SetStageStatusAsync(poNumber, sequence, 'A', ct);
                await _audit.InsertAsync(poNumber, 'P', 'A', userId, note, sequence, roleCode: null, category: null, ct);

                // 2) If all approved: finalize locally and mirror in our header table.
                var allApproved = await _repo.AllStagesApprovedAsync(poNumber, ct);
                if (allApproved)
                {
                    await _repo.FinalizeChainAsync(poNumber, 'A', ct);
                    await _db.Database.ExecuteSqlRawAsync("UPDATE dbo.PO_Header SET Status = 'A' WHERE PoNumber = {0};", poNumber);
                    await _audit.InsertAsync(poNumber, 'P', 'A', "system", "Chain finalized (approved)", null, null, null, ct);

                    // 3) PRMS write-back per new contract:
                    //    - Write INPVP500 audit row (P5STAT = 'A')
                    //    - Fire INPTP500 trigger row
                    var (amountType, bracket, approver) =
                        await _prmsAuditResolver.ResolveAsync(poNumber, sequence, userId, ct);

                    await _prmsWriter.WriteApprovalAuditAsync(
                        poNumber,
                        amountType,
                        bracket,
                        approver,
                        Decision.Approve,
                        DateTime.UtcNow,
                        ct);

                    await _prmsWriter.FireTriggerAsync(poNumber, DateTime.UtcNow, ct);

                    _log.Info($"PO {poNumber} finalized locally (A), PRMS audit inserted and trigger fired.");
                }
                else
                {
                    // 3) Otherwise, notify the next approver (no PRMS side-effects yet).
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

        /// <summary>
        /// Denies a single stage and immediately finalizes the chain locally ('D').
        /// No PRMS write-back on deny per the new rules.
        /// Throws <see cref="ChainNotPendingException"/> when the chain is not in 'P'.
        /// </summary>
        /// <param name="poNumber">PO identifier.</param>
        /// <param name="sequence">Stage sequence being denied.</param>
        /// <param name="userId">Acting user.</param>
        /// <param name="note">Optional audit note.</param>
        /// <param name="ct">Cancellation token.</param>
        public async Task DenyAsync(string poNumber, int sequence, string userId, string? note, CancellationToken ct)
        {
            await using var tx = await _db.Database.BeginTransactionAsync(ct);

            try
            {
                // Guard & lock the chain row to prevent concurrent finalize attempts.
                _ = await LoadChainForUpdateAsync(poNumber, ct);

                // 1) Local stage decision + audit
                await _repo.SetStageStatusAsync(poNumber, sequence, 'D', ct);
                await _audit.InsertAsync(poNumber, 'P', 'D', userId, note, sequence, roleCode: null, category: null, ct);

                // 2) Finalize chain locally as Denied + audit
                await _repo.FinalizeChainAsync(poNumber, 'D', ct);
                await _db.Database.ExecuteSqlRawAsync("UPDATE dbo.PO_Header SET Status = 'D' WHERE PoNumber = {0};", poNumber);
                await _audit.InsertAsync(poNumber, 'P', 'D', "system", "Chain finalized (denied)", null, null, null, ct);

                // 3) No PRMS write-back on deny by design
                _log.Info($"PO {poNumber} finalized locally (D). No PRMS side-effects executed.");

                await tx.CommitAsync(ct);
            }
            catch
            {
                await tx.RollbackAsync(ct);
                throw;
            }
        }

        /// <summary>
        /// Loads the approval chain row with an update lock and ensures it's still pending.
        /// Throws if not found or already finalized.
        /// </summary>
        private async Task<PoApprovalChain> LoadChainForUpdateAsync(string poNumber, CancellationToken ct)
        {
            // UPDLOCK + HOLDLOCK provides a serializable-like lock on this row for the transaction duration.
            var chain = await _db.PoApprovalChains
                .FromSqlRaw(
                    "SELECT * FROM dbo.PO_ApprovalChain WITH (UPDLOCK, HOLDLOCK) WHERE PoNumber = {0}",
                    poNumber)
                .SingleOrDefaultAsync(ct);

            if (chain is null)
                throw new InvalidOperationException($"Approval chain not found for PO {poNumber}.");

            if (chain.Status != 'P')
                throw new ChainNotPendingException(poNumber, chain.Status);

            return chain;
        }
    }
}
