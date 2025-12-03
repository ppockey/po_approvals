using ITT.Logger.Abstractions;
using ITTPortal.POApprovals.Abstraction;
using ITTPortal.POApprovals.Mappers;
using Microsoft.Extensions.Configuration;
using System.Data.Odbc;
using System.Globalization;

namespace ITTPortal.POApprovals.Infrastructure
{
    /// <summary>
    /// ODBC writer implementation for PRMS side-effects:
    ///  - Mark extracted (P3XDTE/P3XTIM)
    ///  - Write approval audit (INPVP500)
    ///  - Fire trigger (INPTP500)
    /// The SQL here is fully parameterized and typed to avoid injection and coercion issues.
    /// </summary>
    public sealed class PrmsWriter : IPrmsWriter
    {
        private readonly string _conn;
        private readonly ILoggerService _log;

        public PrmsWriter(IConfiguration cfg, ILoggerService log)
        {
            _log = log ?? throw new ArgumentNullException(nameof(log));
            var raw = cfg?.GetConnectionString("PrmsOdbc")
                ?? throw new InvalidOperationException("Missing connection string: PrmsOdbc");
            _conn = new OdbcConnectionStringBuilder(raw).ConnectionString;
        }

        /// <inheritdoc />
        public async Task<bool> TryMarkExtractedAsync(string poNumber, DateTime whenUtc, CancellationToken ct)
        {
            // PRMS expects YYMMDD and HHMMSS in integer-typed fields
            int yymmdd = int.Parse(whenUtc.ToString("yyMMdd", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
            int hhmmss = int.Parse(whenUtc.ToString("HHmmss", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);

            const string SQL = @"
                UPDATE CORP400D.GPIMI701.INPUP500
                   SET P3XDTE = ?, P3XTIM = ?
                 WHERE P3PURCH = ?
                   AND P3STAT  = 'W'
                   AND P3XDTE  = 0
                   AND P3XTIM  = 0";

            await using var cn = new OdbcConnection(_conn);
            await cn.OpenAsync(ct);
            await using var cmd = cn.CreateCommand();
            cmd.CommandText = SQL;

            // typed parameters for safety
            cmd.Parameters.Add(new OdbcParameter("@d", OdbcType.Int) { Value = yymmdd });
            cmd.Parameters.Add(new OdbcParameter("@t", OdbcType.Int) { Value = hhmmss });
            cmd.Parameters.Add(new OdbcParameter("@po", OdbcType.VarChar, 20) { Value = poNumber });

            var rows = await cmd.ExecuteNonQueryAsync(ct);
            var claimed = rows > 0;
            _log.Info($"PRMS TryMarkExtracted: po={poNumber}, claimed={claimed}, yymmdd={yymmdd}, hhmmss={hhmmss}");
            return claimed;
        }

        /// <inheritdoc />
        //public async Task WriteApprovalAuditAsync(
        //    string poNumber,
        //    char amountType,
        //    decimal bracket,
        //    string approver,
        //    Decision decision,
        //    DateTime whenUtc,
        //    CancellationToken ct)
        //{
        //    // Convert domain decision → PRMS workfile code (A/R for audit)
        //    var p5 = StatusMaps.ToPrmsP5(decision);

        //    int yymmdd = int.Parse(whenUtc.ToString("yyMMdd", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        //    int hhmmss = int.Parse(whenUtc.ToString("HHmmss", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);

        //    const string SQL = @"
        //        INSERT INTO CORP400D.GPIMI701.INPVP500
        //            (P5PURCH, P5TYPE, P5BRK, P5APRV, P5ADTE, P5ATIM, P5STAT)
        //        VALUES (?, ?, ?, ?, ?, ?, ?)";

        //    await using var cn = new OdbcConnection(_conn);
        //    await cn.OpenAsync(ct);
        //    await using var cmd = cn.CreateCommand();
        //    cmd.CommandText = SQL;

        //    // specify size and precision to avoid truncation or rounding issues
        //    cmd.Parameters.Add(new OdbcParameter("@po", OdbcType.VarChar, 20) { Value = poNumber });
        //    cmd.Parameters.Add(new OdbcParameter("@typ", OdbcType.Char, 1) { Value = amountType });
        //    var pBrk = new OdbcParameter("@brk", OdbcType.Decimal)
        //    {
        //        Precision = 19,
        //        Scale = 4,
        //        Value = bracket
        //    };
        //    cmd.Parameters.Add(pBrk);
        //    cmd.Parameters.Add(new OdbcParameter("@apr", OdbcType.VarChar, 30) { Value = approver });
        //    cmd.Parameters.Add(new OdbcParameter("@d", OdbcType.Int) { Value = yymmdd });
        //    cmd.Parameters.Add(new OdbcParameter("@t", OdbcType.Int) { Value = hhmmss });
        //    cmd.Parameters.Add(new OdbcParameter("@st", OdbcType.Char, 1) { Value = p5 });

        //    var rows = await cmd.ExecuteNonQueryAsync(ct);
        //    if (rows != 1)
        //        throw new InvalidOperationException($"INPVP500 insert failed for PO {poNumber} (rows={rows}).");

        //    _log.Info($"PRMS WriteApprovalAudit: po={poNumber}, type={amountType}, bracket={bracket}, approver={approver}, p5={p5}, yymmdd={yymmdd}, hhmmss={hhmmss}");
        //}

        public async Task WriteApprovalAuditAsync(
            string poNumber,
            char amountType,
            decimal bracket,
            string approver,
            Decision decision,
            DateTime whenUtc,
            CancellationToken ct)
        {
            // Convert domain decision → PRMS workfile code (A/R for audit)
            var p5 = StatusMaps.ToPrmsP5(decision);

            int yymmdd = int.Parse(whenUtc.ToString("yyMMdd", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
            int hhmmss = int.Parse(whenUtc.ToString("HHmmss", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);

            // === IMPORTANT: set these to match your PRMS schema ===
            // Common PRMS lengths are shown; adjust if your file layout differs.
            const int P5PURCH_MAX = 10; // e.g., 6–10 is typical. Value "713997" fits.
            const int P5APRV_MAX = 10; // PRMS approver id length (often 10).
                                       // ================================================

            // Sanitize inputs to avoid DB2 right-truncation
            var po = (poNumber ?? string.Empty).Trim();
            if (po.Length > P5PURCH_MAX) po = po[..P5PURCH_MAX];

            var apr = (approver ?? string.Empty).Trim().ToUpperInvariant();
            if (apr.Length > P5APRV_MAX) apr = apr[..P5APRV_MAX];

            const string SQL = @"
        INSERT INTO CORP400D.GPIMI701.INPVP500
            (P5PURCH, P5TYPE, P5BRK, P5APRV, P5ADTE, P5ATIM, P5STAT)
        VALUES (?, ?, ?, ?, ?, ?, ?)";

            await using var cn = new OdbcConnection(_conn);
            await cn.OpenAsync(ct);
            await using var cmd = cn.CreateCommand();
            cmd.CommandText = SQL;

            // P5PURCH
            cmd.Parameters.Add(new OdbcParameter("@po", OdbcType.VarChar, P5PURCH_MAX) { Value = po });
            // P5TYPE
            cmd.Parameters.Add(new OdbcParameter("@typ", OdbcType.Char, 1) { Value = amountType });
            // P5BRK (match target DEC(p,s); 19,4 is safe upward, DB2 will coerce if wider)
            var pBrk = new OdbcParameter("@brk", OdbcType.Decimal)
            {
                Precision = 19,
                Scale = 4,
                Value = bracket
            };
            cmd.Parameters.Add(pBrk);
            // P5APRV
            cmd.Parameters.Add(new OdbcParameter("@apr", OdbcType.VarChar, P5APRV_MAX) { Value = apr });
            // P5ADTE/P5ATIM
            cmd.Parameters.Add(new OdbcParameter("@d", OdbcType.Int) { Value = yymmdd });
            cmd.Parameters.Add(new OdbcParameter("@t", OdbcType.Int) { Value = hhmmss });
            // P5STAT
            cmd.Parameters.Add(new OdbcParameter("@st", OdbcType.Char, 1) { Value = p5 });

            var rows = await cmd.ExecuteNonQueryAsync(ct);
            if (rows != 1)
                throw new InvalidOperationException($"INPVP500 insert failed for PO {po} (rows={rows}).");

            _log.Info($"PRMS WriteApprovalAudit: po={po}, type={amountType}, bracket={bracket}, approver={apr}, p5={p5}, yymmdd={yymmdd}, hhmmss={hhmmss}");
        }


        /// <inheritdoc />
        public async Task FireTriggerAsync(string poNumber, DateTime whenUtc, CancellationToken ct)
        {
            int yymmdd = int.Parse(whenUtc.ToString("yyMMdd", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
            int hhmmss = int.Parse(whenUtc.ToString("HHmmss", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);

            const string SQL = @"
                INSERT INTO CORP400D.GPIMI701.INPTP500
                    (P6PURCH, P6CDTE, P6CTIM)
                VALUES (?, ?, ?)";

            await using var cn = new OdbcConnection(_conn);
            await cn.OpenAsync(ct);
            await using var cmd = cn.CreateCommand();
            cmd.CommandText = SQL;

            cmd.Parameters.Add(new OdbcParameter("@po", OdbcType.VarChar, 20) { Value = poNumber });
            cmd.Parameters.Add(new OdbcParameter("@d", OdbcType.Int) { Value = yymmdd });
            cmd.Parameters.Add(new OdbcParameter("@t", OdbcType.Int) { Value = hhmmss });

            var rows = await cmd.ExecuteNonQueryAsync(ct);
            if (rows != 1)
                throw new InvalidOperationException($"INPTP500 insert failed for PO {poNumber} (rows={rows}).");

            _log.Info($"PRMS FireTrigger: po={poNumber}, yymmdd={yymmdd}, hhmmss={hhmmss}");
        }
    }

    /// <summary>
    /// Minimal no-op writer that satisfies IPrmsWriter for tests or tooling that do not need PRMS side-effects.
    /// </summary>
    internal sealed class NullPrmsWriter : IPrmsWriter
    {
        private readonly ILoggerService _log;
        public NullPrmsWriter(ILoggerService log) => _log = log;

        public Task<bool> TryMarkExtractedAsync(string poNumber, DateTime whenUtc, CancellationToken ct)
        {
            _log.Info($"NullPrmsWriter.TryMarkExtractedAsync({poNumber}) → false (noop)");
            return Task.FromResult(false);
        }

        public Task WriteApprovalAuditAsync(string poNumber, char amountType, decimal bracket, string approver, Decision decision, DateTime whenUtc, CancellationToken ct)
        {
            _log.Info($"NullPrmsWriter.WriteApprovalAuditAsync({poNumber}, {amountType}, {bracket}, {approver}, {decision}) (noop)");
            return Task.CompletedTask;
        }

        public Task FireTriggerAsync(string poNumber, DateTime whenUtc, CancellationToken ct)
        {
            _log.Info($"NullPrmsWriter.FireTriggerAsync({poNumber}) (noop)");
            return Task.CompletedTask;
        }
    }
}
