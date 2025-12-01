using ITTPortal.POApprovals.Models;

namespace ITTPortal.POApprovals.Abstraction
{
    /// <summary>
    /// Writer for PRMS: claim-extract stamps, audit rows, and trigger rows.
    /// </summary>
    public interface IPrmsWriter
    {
        /// <summary>
        /// Attempts to mark the PO as "extracted" by setting P3XDTE/P3XTIM only when it is still waiting (W)
        /// and the stamp columns are zero. Returns true if this call successfully claimed the PO.
        /// </summary>
        Task<bool> TryMarkExtractedAsync(string poNumber, DateTime whenUtc, CancellationToken ct);

        /// <summary>
        /// Writes a single approval-audit line to INPVP500 for the supplied decisive event.
        /// </summary>
        Task WriteApprovalAuditAsync(
            string poNumber,
            char amountType,   // 'D' or 'I'
            decimal bracket,   // e.g., .01, 200, 50000 (set precision/scale)
            string approver,   // PRMS-side approver code/name (e.g., so-gbl-ppockey)
            Decision decision, // domain decision to be mapped to INPVP500.P5STAT
            DateTime whenUtc,
            CancellationToken ct);

        /// <summary>
        /// Inserts a row into INPTP500 to fire PRMS trigger processing for the PO (approve/deny terminal events).
        /// </summary>
        Task FireTriggerAsync(string poNumber, DateTime whenUtc, CancellationToken ct);
    }
}
