using ITTPortal.POApprovals.Models;

namespace ITTPortal.POApprovals.Abstraction
{
    /// <summary>
    /// Reader for PRMS: enumerates POs currently awaiting approval, returning header and lines
    /// </summary>
    public interface IPrmsReader
    {
        /// <summary>
        /// Enumerate POs that PRMS marks as waiting for approval (W) and not yet extracted (P3XDTE/P3XTIM = 0).
        /// The implementation will claim each PO before yielding it to the caller 
        /// so that parallel workers do not process the same PO
        /// </summary>
        IAsyncEnumerable<(SqlPoHeader Header, SqlPoLine[] Lines)> ReadWaitingApprovalAsync(CancellationToken ct);
    }
}
