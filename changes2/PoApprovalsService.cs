using ITT.Logger.Abstractions;
using ITTPortal.Core.Abstractions;
using ITTPortal.Core.Entities.POApprovals;
using ITTPortal.Infrastructure;
using ITTPortal.POApprovals.Abstraction;
using ITTPortal.POApprovals.Mappers;
using Microsoft.EntityFrameworkCore;
using System.Data; // IsolationLevel
using System.Linq;

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
    /// Supplies PRMS-specific inputs required to write the approval audit row.
    /// </summary>
    public interface IPrmsAuditInputResolver
    {
        Task<(char amountType, decimal bracket, string approver)> ResolveAsync(
            string poNumber, int sequence, string userId, CancellationToken ct);
    }

    public sealed class PrmsAuditInputResolverFromStage : IPrmsAuditInputResolver
    {
        private readonly PortalDbContext _db;

        public PrmsAuditInputResolverFromStage(PortalDbContext db) => _db = db;

        public async Task<(char amountType, decimal bracket, string approver)> ResolveAsync(
            string poNumber, int sequence, string userId, CancellationToken ct)
        {
            var stage = await _db.PoApprovalStages
                .AsNoTracking()
                .SingleOrDefaultAsync(s => s.PoNumber == poNumber && s.Sequence == sequence, ct)
                ?? throw new InvalidOperationException($"Stage not found for {poNumber} seq {sequence}");

            //static string MapUserIdToPrmsApprover(string uid)
            //{
            //    if (string.IsNullOrWhiteSpace(uid)) return "SYSTEM";
            //    var at = uid.IndexOf('@');
            //    var core = at > 0 ? uid[..at] : uid;
            //    return core.Trim().ToUpperInvariant();
            //}
            static string MapUserIdToPrmsApprover(string userId)
            {
                const int MAX = 10; // match INPVP500.P5APRV
                if (string.IsNullOrWhiteSpace(userId)) return "SYSTEM";

                var at = userId.IndexOf('@');
                var core = at > 0 ? userId[..at] : userId;
                // keep A–Z/0–9 only, then uppercase
                var filtered = new string(core.Where(ch => char.IsLetterOrDigit(ch)).ToArray()).ToUpperInvariant();
                if (filtered.Length == 0) filtered = "SYSTEM";
                return filtered.Length > MAX ? filtered[..MAX] : filtered;
            }


        var approver = MapUserIdToPrmsApprover(userId);

            char amtType;
            decimal bracket;

            if (stage.Category == 'I')
            {
                amtType = 'I';
                bracket = await _db.PoDelegationOfAuthorityIndirectExpenses
                    .AsNoTracking()
                    .Where(x => x.Level == stage.RoleCode)
                    .Select(x => (decimal?)x.Amount)
                    .SingleOrDefaultAsync(ct) ?? 0m;
            }
            else if (stage.Category == 'D')
            {
                amtType = 'D';
                bracket = await _db.PoDelegationOfAuthorityDirectMaterials
                    .AsNoTracking()
                    .Where(x => x.Level == stage.RoleCode)
                    .Select(x => (decimal?)x.Amount)
                    .SingleOrDefaultAsync(ct) ?? 0m;
            }
            else
            {
                var d = await _db.PoDelegationOfAuthorityDirectMaterials
                    .AsNoTracking()
                    .Where(x => x.Level == stage.RoleCode)
                    .Select(x => (decimal?)x.Amount)
                    .SingleOrDefaultAsync(ct);

                var i = await _db.PoDelegationOfAuthorityIndirectExpenses
                    .AsNoTracking()
                    .Where(x => x.Level == stage.RoleCode)
                    .Select(x => (decimal?)x.Amount)
                    .SingleOrDefaultAsync(ct);

                if (d.HasValue && (!i.HasValue || d.Value >= i.Value))
                {
                    amtType = 'D'; bracket = d.Value;
                }
                else if (i.HasValue)
                {
                    amtType = 'I'; bracket = i.Value;
                }
                else
                {
                    amtType = 'D'; bracket = 0m;
                }
            }

            return (amtType, bracket, approver);
        }
    }

    /// <summary>
    /// Orchestrates local PO approval workflow and PRMS side-effects.
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

        public PoApprovalsService(IPoApprovalChainRepository repo,
                                  IPoApprovalAuditRepository audit,
                                  IPoApprovalNotifier notifier,
                                  IPrmsWriter prmsWriter,
                                  PortalDbContext db,
                                  ILoggerService log)
            : this(repo, audit, notifier, prmsWriter, new PrmsAuditInputResolverFromStage(db), db, log)
        { }

        public async Task ApproveAsync(string poNumber, int sequence, string userId, string? note, CancellationToken ct)
        {
            // Variables to use AFTER commit (outside the retryable delegate)
            bool chainFinalized = false;
            (int sequence, string roleCode)? nextToNotify = null;
            (char amountType, decimal bracket, string approver) prmsInputs = default;

            var strategy = _db.Database.CreateExecutionStrategy();
            await strategy.ExecuteAsync(async () =>
            {
                await using var tx = await _db.Database.BeginTransactionAsync(IsolationLevel.Serializable, ct);
                try
                {
                    _ = await LoadChainForUpdateAsync(poNumber, ct);

                    await _repo.SetStageStatusAsync(poNumber, sequence, 'A', ct);
                    await _audit.InsertAsync(poNumber, 'P', 'A', userId, note, sequence, roleCode: null, category: null, ct);

                    var allApproved = await _repo.AllStagesApprovedAsync(poNumber, ct);
                    if (allApproved)
                    {
                        await _repo.FinalizeChainAsync(poNumber, 'A', ct);

                        var hdr = await _db.PoHeaders.SingleOrDefaultAsync(h => h.PoNumber == poNumber, ct)
                                  ?? throw new InvalidOperationException($"Header not found for PO {poNumber}.");
                        hdr.Status = 'A';
                        await _db.SaveChangesAsync(ct);

                        await _audit.InsertAsync(poNumber, 'P', 'A', "system", "Chain finalized (approved)", null, null, null, ct);
                        chainFinalized = true;
                    }
                    else
                    {
                        //var s = await _repo.GetFirstPendingStageAsync(poNumber, ct);
                        //if (s is not null)
                        //    nextToNotify = (s.Sequence, s.RoleCode);
                        var next = await _repo.GetFirstPendingStageAsync(poNumber, ct);
                        if (next is { } s)
                            await _notifier.NotifyStageReadyAsync(poNumber, s.Seq, s.RoleCode, ct);
                        
                    }

                    // Capture PRMS inputs but DO NOT call PRMS inside the transaction
                    prmsInputs = await _prmsAuditResolver.ResolveAsync(poNumber, sequence, userId, ct);

                    await tx.CommitAsync(ct);
                }
                catch
                {
                    await tx.RollbackAsync(ct);
                    throw;
                }
            });

            // OUTSIDE the retryable/transactional block: side effects that must not be retried by EF
            await _prmsWriter.WriteApprovalAuditAsync(
                poNumber,
                prmsInputs.amountType,
                prmsInputs.bracket,
                prmsInputs.approver,
                Decision.Approve,
                DateTime.UtcNow,
                ct);

            if (chainFinalized)
            {
                await _prmsWriter.FireTriggerAsync(poNumber, DateTime.UtcNow, ct);
            }
            else if (nextToNotify.HasValue)
            {
                await _notifier.NotifyStageReadyAsync(poNumber, nextToNotify.Value.sequence, nextToNotify.Value.roleCode, ct);
            }
        }

        public async Task DenyAsync(string poNumber, int sequence, string userId, string? note, CancellationToken ct)
        {
            (char amountType, decimal bracket, string approver) prmsInputs = default;

            var strategy = _db.Database.CreateExecutionStrategy();
            await strategy.ExecuteAsync(async () =>
            {
                await using var tx = await _db.Database.BeginTransactionAsync(IsolationLevel.Serializable, ct);
                try
                {
                    _ = await LoadChainForUpdateAsync(poNumber, ct);

                    await _repo.SetStageStatusAsync(poNumber, sequence, 'D', ct);
                    await _audit.InsertAsync(poNumber, 'P', 'D', userId, note, sequence, roleCode: null, category: null, ct);

                    await _repo.FinalizeChainAsync(poNumber, 'D', ct);

                    var hdr = await _db.PoHeaders.SingleOrDefaultAsync(h => h.PoNumber == poNumber, ct)
                              ?? throw new InvalidOperationException($"Header not found for PO {poNumber}.");
                    hdr.Status = 'D';
                    await _db.SaveChangesAsync(ct);

                    await _audit.InsertAsync(poNumber, 'P', 'D', "system", "Chain finalized (denied)", null, null, null, ct);

                    // Capture PRMS inputs for this stage
                    prmsInputs = await _prmsAuditResolver.ResolveAsync(poNumber, sequence, userId, ct);

                    await tx.CommitAsync(ct);
                }
                catch
                {
                    await tx.RollbackAsync(ct);
                    throw;
                }
            });

            // Post-commit PRMS side-effects
            await _prmsWriter.WriteApprovalAuditAsync(
                poNumber,
                prmsInputs.amountType,
                prmsInputs.bracket,
                prmsInputs.approver,
                Decision.Deny,
                DateTime.UtcNow,
                ct);

            await _prmsWriter.FireTriggerAsync(poNumber, DateTime.UtcNow, ct);
        }

        /// <summary>
        /// Loads the approval chain row and ensures it's still pending.
        /// </summary>
        private async Task<PoApprovalChain> LoadChainForUpdateAsync(string poNumber, CancellationToken ct)
        {
            var chain = await _db.PoApprovalChains
                .Where(c => c.PoNumber == poNumber)
                .SingleOrDefaultAsync(ct);

            if (chain is null)
                throw new InvalidOperationException($"Approval chain not found for PO {poNumber}.");

            if (chain.Status != 'P')
                throw new ChainNotPendingException(poNumber, chain.Status);

            return chain;
        }
    }
}
