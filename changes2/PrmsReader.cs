using ITT.Logger.Abstractions;
using ITTPortal.POApprovals.Abstraction;
using ITTPortal.POApprovals.Models;
using Microsoft.Extensions.Configuration;
using System.Data.Odbc;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace ITTPortal.POApprovals.Infrastructure
{
    /// <summary>
    /// ODBC reader for PRMS
    ///  1) Reads candidate POs from INPUL500 filtered to Waiting + not-extracted (W + zero stamps)
    ///  2) For each, loads Header and Lines from INPOL112/INPOL300
    ///  3) attempts to "claim" the PO by writing P3XDTE/P3XTIM (only if header exists)
    ///     If the claim fails (another worker won), the PO is skipped
    /// 
    /// Yielded items are thus "owned" by this worker, minimizing duplicate processing.
    /// </summary>
    public sealed partial class PrmsReader : IPrmsReader
    {
        private const string app = "ITTPortal.POApprovals.Infrastructure.PrmsReader";

        private readonly string _connString;
        private readonly ILoggerService _log;
        private readonly IConfiguration _cfg;
        private readonly IPrmsWriter _prmsWriter;

        /// <summary>
        /// Preferred ctor – supply IPrmsWriter to enable claim-stamping and audit/trigger usage.
        /// </summary>
        public PrmsReader(IConfiguration cfg, ILoggerService log, IPrmsWriter prmsWriter)
        {
            _cfg = cfg ?? throw new ArgumentNullException(nameof(cfg));
            _log = log ?? throw new ArgumentNullException(nameof(log));
            _prmsWriter = prmsWriter ?? throw new ArgumentNullException(nameof(prmsWriter));

            var raw = cfg.GetConnectionString("PrmsOdbc")
                ?? throw new InvalidOperationException("Missing connection string: PrmsOdbc");
            _connString = new OdbcConnectionStringBuilder(raw).ConnectionString;

            _log.Info($"{app} Initialized PrmsReader with ODBC connection.");
        }

        /// <summary>
        /// Backward-compatible ctor used by older tests; injects a NullPrmsWriter internally.
        /// </summary>
        public PrmsReader(IConfiguration cfg, ILoggerService log)
            : this(cfg, log, new NullPrmsWriter(log))
        { }

        /// <inheritdoc />
        public async IAsyncEnumerable<(SqlPoHeader Header, SqlPoLine[] Lines)>
            ReadWaitingApprovalAsync([EnumeratorCancellation] CancellationToken ct)
        {
            // 1) Candidate seed: Waiting + not yet extracted -- See Yelena requirement #1
            const string Q_WAITING_SEED = @"
                select distinct P3PURCH, P3DAMNT, P3IAMNT
                  from CORP400D.GPIMI701.INPUL500
                 where P3STAT = 'W' and P3XDTE = 0 and P3XTIM = 0";

            // 2) Header
            const string Q_HEADER = @"
                select a.PURCH, a.VNDNO, a.HOUSE, a.BUYER, b.BMNAM as BUYERNAME,
                       v.VNAME, v.VADD1, v.VADD2, v.VADD3, v.VSTAT, v.VZIPC,
                       a.PODMN, a.PODDY, a.PODYR
                  from CORP400D.GPIMI701.INPOL112 a
                  join CORP400D.GPIMI701.MSVMP100 v on a.VNDNO = v.VNDNO
                  join CORP400D.GPIMI701.POBMP100 b on a.BUYER = b.BMBUY
                 where a.PURCH = ?";

            // 3) Lines
            const string Q_LINES = @"
                select a.PURCH,
                       a.""LINE#"" as LINE_NO,
                       a.HOUSE,
                       a.PRDNO,
                       p.DESCP as ItemDescription,
                       a.SDESC as ItemShortDescription,
                       a.QUANO,
                       a.ORDUM,
                       a.ECOST,
                       (a.QUANO * a.ECOST) as EstValue,
                       a.RQ3MN, a.RQ3DY, a.RQ3YR,
                       a.POIGL
                  from CORP400D.GPIMI701.INPOL300 a
                  left join CORP400D.GPIMI701.MSPMP100 p on a.PRDNO = p.PRDNO
                 where a.PURCH = ?
                 order by a.""LINE#""";

            _log.Info($"{app} Starting ReadWaitingApprovalAsync.");

            await using var cn = new OdbcConnection(_connString);
            try
            {
                await cn.OpenAsync(ct).ConfigureAwait(false);
                _log.Info($"{app} Opened ODBC connection to PRMS.");
            }
            catch (Exception exOpen)
            {
                _log.Fatal($"{app} Failed to open ODBC connection to PRMS.", exception: exOpen);
                throw;
            }

            var candidates = new List<(string Po, decimal? Dir, decimal? Ind)>(capacity: 256);

            // --------- Seed read ----------
            try
            {
                using var cmd = cn.CreateCommand();
                cmd.CommandText = Q_WAITING_SEED;

                using var rdr = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                int seedCount = 0;

                while (await rdr.ReadAsync(ct).ConfigureAwait(false))
                {
                    static string? TrimStr(object? o)
                    {
                        if (o == null || o is DBNull) return null;
                        var s = o.ToString()?.Trim();
                        return string.IsNullOrWhiteSpace(s) ? null : s;
                    }

                    static decimal? ToDec(object? o)
                    {
                        if (o == null || o is DBNull) return null;
                        return decimal.TryParse(o.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var d) ? d : null;
                    }

                    var po = TrimStr(rdr["P3PURCH"]);
                    if (!string.IsNullOrEmpty(po))
                    {
                        candidates.Add((po, ToDec(rdr["P3DAMNT"]), ToDec(rdr["P3IAMNT"])));
                        seedCount++;
                    }
                }

                _log.Info($"{app} Seed read complete. Candidate POs found: {seedCount}.");
            }
            catch (OperationCanceledException)
            {
                _log.Warning($"{app} Cancellation requested during seed read.");
                throw;
            }
            catch (Exception exSeed)
            {
                _log.Error($"{app} Error while reading seed from INPUL500.", exception: exSeed);
                throw;
            }

            // --------- Expand each candidate ----------
            foreach (var c in candidates)
            {
                if (ct.IsCancellationRequested)
                {
                    _log.Warning($"{app} Cancellation requested; stopping before next candidate.");
                    ct.ThrowIfCancellationRequested();
                }

                _log.Info($"{app} Expanding PO '{c.Po}'.");

                // ---- Header (read first; only attempt to claim if header exists) ----
                SqlPoHeader? header = null;
                try
                {
                    using var cmdH = cn.CreateCommand();
                    cmdH.CommandText = Q_HEADER;
                    cmdH.Parameters.Add(new OdbcParameter("@purch", OdbcType.VarChar, 20) { Value = c.Po });

                    using var rdrH = await cmdH.ExecuteReaderAsync(ct).ConfigureAwait(false);
                    if (await rdrH.ReadAsync(ct).ConfigureAwait(false))
                    {
                        static string? S(object o) => o is DBNull ? null : o?.ToString()?.Trim();

                        static int? I(object o)
                        {
                            if (o is DBNull) return null;
                            return int.TryParse(o.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var v) ? v : null;
                        }

                        static DateTime? FromYmd(int? yy, int? mm, int? dd)
                        {
                            if (!yy.HasValue || !mm.HasValue || !dd.HasValue) return null;
                            try
                            {
                                var year = (yy.Value >= 0 && yy.Value <= 69) ? 2000 + yy.Value : 1900 + yy.Value;
                                return new DateTime(year, Math.Clamp(mm.Value, 1, 12), Math.Clamp(dd.Value, 1, 31));
                            }
                            catch { return null; }
                        }

                        header = new SqlPoHeader(
                            PoNumber: S(rdrH["PURCH"]) ?? c.Po,
                            PoDate: FromYmd(I(rdrH["PODYR"]), I(rdrH["PODMN"]), I(rdrH["PODDY"])),
                            VendorNumber: S(rdrH["VNDNO"]) ?? string.Empty,
                            VendorName: S(rdrH["VNAME"]),
                            VendorAddr1: S(rdrH["VADD1"]),
                            VendorAddr2: S(rdrH["VADD2"]),
                            VendorAddr3: S(rdrH["VADD3"]),
                            VendorState: S(rdrH["VSTAT"]),
                            VendorPostalCode: S(rdrH["VZIPC"]),
                            BuyerCode: S(rdrH["BUYER"]),
                            BuyerName: S(rdrH["BUYERNAME"]),
                            HouseCode: S(rdrH["HOUSE"]),
                            DirectAmount: c.Dir,
                            IndirectAmount: c.Ind,
                            CreatedAtUtc: DateTime.UtcNow
                        );
                    }
                    else
                    {
                        _log.Warning($"{app} Header not found for PO '{c.Po}'. Skipping.");
                    }
                }
                catch (OperationCanceledException)
                {
                    _log.Warning($"{app} Cancellation during header read for PO '{c.Po}'.");
                    throw;
                }
                catch (Exception exHdr)
                {
                    _log.Error($"{app} Error reading header for PO '{c.Po}'.", exception: exHdr);
                    throw;
                }

                if (header is null) continue;

                // ---- Attempt to claim AFTER header is known to exist See Yelena requirement # 2 ----
                bool claimed = await _prmsWriter.TryMarkExtractedAsync(c.Po, DateTime.UtcNow, ct);
                if (!claimed)
                {
                    _log.Info($"{app} Claim failed; another worker already claimed PO '{c.Po}'. Skipping.");
                    continue;
                }

                // ---- Lines ----
                var lines = new List<SqlPoLine>(32);
                try
                {
                    using var cmdL = cn.CreateCommand();
                    cmdL.CommandText = Q_LINES;
                    cmdL.Parameters.Add(new OdbcParameter("@purch", OdbcType.VarChar, 20) { Value = c.Po });

                    using var rdrL = await cmdL.ExecuteReaderAsync(ct).ConfigureAwait(false);
                    int lineCount = 0;

                    while (await rdrL.ReadAsync(ct).ConfigureAwait(false))
                    {
                        static string? S(object o) => o is DBNull ? null : o?.ToString()?.Trim();

                        static int? I(object o)
                        {
                            if (o is DBNull) return null;
                            return int.TryParse(o.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var v) ? v : null;
                        }

                        static decimal? D(object o)
                        {
                            if (o is DBNull) return null;
                            return decimal.TryParse(o.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var v) ? v : null;
                        }

                        static DateTime? FromYmd(int? yy, int? mm, int? dd)
                        {
                            if (!yy.HasValue || !mm.HasValue || !dd.HasValue) return null;
                            try
                            {
                                var year = (yy.Value >= 0 && yy.Value <= 69) ? 2000 + yy.Value : 1900 + yy.Value;
                                return new DateTime(year, Math.Clamp(mm.Value, 1, 12), Math.Clamp(dd.Value, 1, 31));
                            }
                            catch { return null; }
                        }

                        var qty = D(rdrL["QUANO"]);
                        var unit = D(rdrL["ECOST"]);
                        var ext = D(rdrL["EstValue"]) ?? (qty.HasValue && unit.HasValue ? qty.Value * unit.Value : (decimal?)null);

                        lines.Add(new SqlPoLine(
                            PoNumber: c.Po,
                            LineNumber: I(rdrL["LINE_NO"]) ?? 0,
                            HouseCode: S(rdrL["HOUSE"]),
                            ItemNumber: S(rdrL["PRDNO"]),
                            ItemDescription: S(rdrL["ItemDescription"]),
                            ItemShortDescription: S(rdrL["ItemShortDescription"]),
                            QuantityOrdered: qty,
                            OrderUom: S(rdrL["ORDUM"]),
                            UnitCost: unit,
                            ExtendedCost: ext,
                            RequiredDate: FromYmd(I(rdrL["RQ3YR"]), I(rdrL["RQ3MN"]), I(rdrL["RQ3DY"])),
                            GlAccount: S(rdrL["POIGL"])
                        ));
                        lineCount++;
                    }

                    _log.Info($"{app} Lines fetched for PO '{c.Po}': {lineCount}.");
                }
                catch (OperationCanceledException)
                {
                    _log.Warning($"{app} Cancellation during lines read for PO '{c.Po}'.");
                    throw;
                }
                catch (Exception exLines)
                {
                    _log.Error($"{app} Error reading lines for PO '{c.Po}'.", exception: exLines);
                    throw;
                }

                // ---- Emit the claimed, fully-read PO payload ----
                yield return (header, lines.ToArray());
            }

            _log.Info($"{app} Completed ReadWaitingApprovalAsync: processed {candidates.Count} candidate POs.");
        }
    }
}
