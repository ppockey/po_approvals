using ITT.Logger.Abstractions;
using ITTPortal.Core.Abstractions;
using ITTPortal.POApprovals.Abstraction;

namespace ITTPortal.POApprovals.Services
{
    public sealed class ProcessApprovalOutboxJob : IProcessApprovalOutboxJob
    {
        private const string EVENT_TYPE_NEW_WAITING = "PO_NEW_WAITING";
        private const int DEFAULT_BATCH_SIZE = 25;

        private readonly IPoApprovalOutboxRepository _outbox;
        private readonly IPoApprovalsApprovalStageRepository _stages;
        private readonly IPoHeaderLookupRepository _headers;
        private readonly IPoApprovalApproverDirectoryRepository _directory;
        private readonly IPoApprovalNotifier _notifier;
        private readonly ILoggerService _log;

        public ProcessApprovalOutboxJob(
            IPoApprovalOutboxRepository outbox,
            IPoApprovalsApprovalStageRepository stages,
            IPoHeaderLookupRepository headers,
            IPoApprovalApproverDirectoryRepository directory,
            IPoApprovalNotifier notifier,
            ILoggerService log)
        {
            _outbox = outbox ?? throw new ArgumentNullException(nameof(outbox));
            _stages = stages ?? throw new ArgumentNullException(nameof(stages));
            _headers = headers ?? throw new ArgumentNullException(nameof(headers));
            _directory = directory ?? throw new ArgumentNullException(nameof(directory));
            _notifier = notifier ?? throw new ArgumentNullException(nameof(notifier));
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        public async Task RunAsync(CancellationToken ct)
        {
            // Keep batch size small for safety; make configurable later if desired.
            const int top = DEFAULT_BATCH_SIZE;

            var events = await _outbox.GetUnprocessedNewWaitingAsync(top, ct);
            if (events.Count == 0)
            {
                _log.Info("[PO Approvals] Outbox job: no unprocessed PO_NEW_WAITING events.", null, null, null, null);
                return;
            }

            foreach (var e in events)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    // 1) Determine which stage is ready to be notified (first pending stage).
                    var firstStage = await _stages.GetFirstPendingStageAsync(e.PoNumber, ct);
                    if (firstStage is null)
                    {
                        // Nothing actionable. Mark processed so we don't retry forever.
                        _log.Info(
                            $"[PO Approvals] PO={e.PoNumber}: no pending stage found; marking outbox processed (nothing to do).",
                            null, null, null, null);

                        await _outbox.MarkProcessedAsync(e.OutboxId, ct);
                        continue;
                    }

                    // 2) Load routing context (CostCenterKey).
                    var costCenterKey = await _headers.GetCostCenterKeyAsync(e.PoNumber, ct);

                    // 3) Resolve recipient using the SAME rule you use everywhere:
                    //    - COST CENTER OWNER/SUPERVISOR: prefer cost-center-specific mapping, else global fallback
                    //    - all other roles: global mapping only
                    var email = await _directory.ResolveEmailForRoleAsync(firstStage.RoleCode, costCenterKey, ct);

                    if (string.IsNullOrWhiteSpace(email))
                    {
                        _log.Info(
                            $"[PO Approvals] PO={e.PoNumber}: No active directory email found for RoleCode '{firstStage.RoleCode}'" +
                            (string.IsNullOrWhiteSpace(costCenterKey) ? "" : $" (CostCenterKey={costCenterKey})") +
                            ". Incrementing attempts; leaving event unprocessed.",
                            null, null, null, null
                        );

                        await _outbox.IncrementAttemptsAsync(e.OutboxId, ct);
                        continue;
                    }

                    // 4) "Would happen" notification (placeholder notifier logs today; later will email).
                    _log.Info(
                        $"[PO Approvals] PO={e.PoNumber}: would notify first approver. Seq={firstStage.Sequence}, Role='{firstStage.RoleCode}', Email='{email}'.",
                        null, null, null, null
                    );

                    await _notifier.NotifyStageReadyAsync(e.PoNumber, firstStage.Sequence, firstStage.RoleCode, ct);

                    // 5) Mark processed only after successful recipient resolution + notifier call.
                    await _outbox.MarkProcessedAsync(e.OutboxId, ct);
                }
                catch (OperationCanceledException)
                {
                    // Preserve cancellation semantics.
                    throw;
                }
                catch (Exception ex)
                {
                    _log.Error(
                        $"[PO Approvals] Outbox job error. PO={e.PoNumber}, OutboxId={e.OutboxId}",
                        null,
                        ex,
                        null,
                        null);

                    // If something failed unexpectedly, increment attempts, but do not block the batch.
                    try
                    {
                        await _outbox.IncrementAttemptsAsync(e.OutboxId, ct);
                    }
                    catch
                    {
                        // Intentionally swallow: best-effort attempt tracking.
                    }
                }
            }
        }
    }
}
