namespace ITTPortal.POApprovals.Models
{
    /// <summary>
    /// High-level decision issued by the frontend
    /// </summary>
    public enum Decision
    {
        Approve,
        Deny
    }

    /// <summary>
    /// High-level state for POs in SQL-side app
    /// </summary>
    public enum PoState
    {
        Waiting,
        Approved,
        Denied
    }

    /// <summary>
    /// mappings between frontend and PRMS
    /// </summary>
    public static class StatusMaps
    {
        /// <summary>
        /// Convert frontend decision char to domain enum. frontend sends 'A' or 'D'.
        /// </summary>
        public static Decision FromUi(char ui) => ui switch
        {
            'A' => Decision.Approve,
            'D' => Decision.Deny,
            _ => throw new ArgumentOutOfRangeException(nameof(ui), $"Unsupported UI decision '{ui}'.")
        };

        /// <summary>
        /// Domain → PRMS INPUP500.P3STAT. PRMS expects 'Y' (approved) / 'N' (denied). Waiting = 'W'.
        /// </summary>
        public static char ToPrmsP3(Decision d) => d switch
        {
            Decision.Approve => 'Y',
            Decision.Deny => 'N',
            _ => throw new ArgumentOutOfRangeException(nameof(d))
        };

        /// <summary>
        /// Domain → PRMS INPVP500.P5STAT (approval audit work file). PRMS expects 'A' (approved) / 'R' (rejected).
        /// </summary>
        public static char ToPrmsP5(Decision d) => d switch
        {
            Decision.Approve => 'A',
            Decision.Deny => 'R',
            _ => throw new ArgumentOutOfRangeException(nameof(d))
        };

        /// <summary>
        /// Domain → App view (optional helper if need to set SQL visible state)
        /// </summary>
        public static char ToAppStatus(PoState s) => s switch
        {
            PoState.Waiting => 'W',
            PoState.Approved => 'A',
            PoState.Denied => 'D',
            _ => throw new ArgumentOutOfRangeException(nameof(s))
        };
    }
}
