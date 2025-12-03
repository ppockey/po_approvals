using ITT.Logger.Abstractions;
using ITT.Logger.Services;
using ITTPortal.Infrastructure;
using ITTPortal.Infrastructure.Repositories;
using ITTPortal.POApprovals.Abstraction;
using ITTPortal.POApprovals.Infrastructure;
using ITTPortal.POApprovals.Mappers;
using ITTPortal.POApprovals.Services;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Extensions.Logging;
using NLog.Web;
using System.Data;
using System.Data.Odbc;
using System.Globalization;

namespace ITTPortal.POApprovals.Tests
{
    [TestClass]
    public class PoApprovalsTests
    {
        private static string _cs = default!;
        private static TestContext? _testContext;
        private static IConfiguration _cfg = default!;
        private static ILoggerService _log = default!;
        private static string _sql = default!;
        private static IConfiguration _dbCfg = default!;
        private static ServiceProvider _sp = default!;

        // =====================
        // POCOs (Strongly-typed)
        // =====================

        /// <summary>
        /// Initial approval "seed" row from INPUL500.
        /// This row tells us which PURCH (PO) to expand into header/lines,
        /// and carries amounts + creation timestamp to help determine approval needs.
        /// </summary>
        public sealed class PoApprovalCandidate
        {
            /// <summary>Purchase Order number (key for joins).</summary>
            public string Purch { get; init; } = "";

            /// <summary>Direct amount (P3DAMNT); compared to INPDP500 for approval level.</summary>
            public decimal? DirectAmount { get; init; }

            /// <summary>Indirect amount (P3IAMNT); compared to INPEP500 for approval level.</summary>
            public decimal? IndirectAmount { get; init; }

            /// <summary>Creation timestamp from P3CDTE (CYYMMDD) + P3CTIM (seconds since midnight).</summary>
            public DateTime? CreatedAt { get; init; }
        }

        /// <summary>
        /// Purchase Order header (INPOL112). Fields here are intentionally minimal;
        /// add/rename after schema is confirmed with your SMEs (Cindy/George).
        /// </summary>
        public sealed class PoHeader
        {
            public string Purch { get; init; } = "";
            public string? Buyer { get; init; }
            public string? VendorNo { get; init; }

            /// <summary>Total cost (commonly FOCST in many PRMS layouts).</summary>
            public decimal? TotalCost { get; init; }

            /// <summary>Approval flag (e.g., P1PAP 'Y'/'N') if present on your schema.</summary>
            public string? ApprovalFlag { get; init; }
        }

        /// <summary>
        /// Purchase Order line (INPOL300). Minimal draft set of fields;
        /// expand to include due dates, sites, etc., as needed.
        /// </summary>
        public sealed class PoLine
        {
            public string Purch { get; init; } = "";

            /// <summary>Line number (column named LINE# on PRMS; requires quoting in SQL).</summary>
            public int? LineNo { get; init; }

            /// <summary>Part number / item code (PRDNO).</summary>
            public string? PartNo { get; init; }

            /// <summary>Ordered quantity (QUANO).</summary>
            public decimal? Qty { get; init; }

            /// <summary>Unit cost (ECOST).</summary>
            public decimal? UnitCost { get; init; }

            /// <summary>Extended cost (EXTCOST) if present; if absent we compute Qty * UnitCost.</summary>
            public decimal? ExtCost { get; init; }
        }

        /// <summary>
        /// Aggregated PO container: Header + Lines + source candidate used to find it.
        /// </summary>
        public sealed class PurchaseOrder
        {
            public PoHeader Header { get; init; } = new PoHeader();
            public List<PoLine> Lines { get; init; } = new();
            public PoApprovalCandidate Source { get; init; } = new PoApprovalCandidate();
        }

        // =====================
        // Test bootstrap
        // =====================

        /// <summary>
        /// Loads the ODBC connection string from appsettings.json ("PrmsOdbc") and saves TestContext.
        /// appsettings.json must be copied to the test output folder.
        /// </summary>
        [ClassInitialize]
        public static void Init(TestContext ctx)
        {
            _testContext = ctx;

            _cfg = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
                .Build();

            LogManager.Setup().LoadConfigurationFromFile("nlog.config");

            var raw = _cfg.GetConnectionString("PrmsOdbc")
                ?? throw new InvalidOperationException("Missing connection string: PrmsOdbc");

            _cs = new OdbcConnectionStringBuilder(raw).ConnectionString;

            var athens = _cfg.GetConnectionString("AthensWebAppsDevSQLServer")
                ?? throw new InvalidOperationException("Missing connection string AthensWebAppsDevSQLServer");

            _dbCfg = new ConfigurationBuilder()
                .AddConfiguration(_cfg)
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:DefaultConnection"] = athens
                }).Build();

            _sql = athens;

            var services = new ServiceCollection();
            services.AddSingleton(_dbCfg);
            services.AddHttpContextAccessor();
            services.AddLogging(b =>
            {
                b.ClearProviders();
                b.AddNLog();

            });

            services.AddSingleton<ILoggerService, LoggerService>();
            _sp = services.BuildServiceProvider();

            _log = _sp.GetRequiredService<ILoggerService>();
        }

        // =====================
        // Test 1: Smoke test
        // =====================

        /// <summary>
        /// Basic connection + query "smoke test" against INPOP* + related tables.
        /// Verifies the ODBC plumbing is OK by executing a scalar.
        /// </summary>
        [TestMethod]
        public async Task Can_Open_Odbc_Connection_And_Query()
        {
            using var cn = new OdbcConnection(_cs);
            await cn.OpenAsync();
            _testContext!.WriteLine("ODBC connection opened successfully.");

            using var cmd = cn.CreateCommand();
            cmd.CommandText = """
                                select BMNAM
                                ,a.PURCH
                                ,LINE#
                                ,a.PRDNO
                                ,a.PODMN
                                ,a.PODDY
                                ,a.PODYR
                                ,quano
                                ,ECOST
                                ,quano*ECOST as ExtCost
                                ,a.OREMN
                                ,a.OREDY
                                ,a.OREYR
                                ,e.FOCST as TotalPurchaseOrderCost
                                ,a.BUYER
                                ,f.VNAME
                                ,RRN(a) as RelativeRecordNum
                                from CORP400D.GPIMI701.INPOP300 a
                                inner join CORP400D.GPIMI701.MSPMP100 b on a.PRDNO = b.PRDNO
                                inner join CORP400D.GPIMI701.POBMP100 ba on a.BUYER = ba.BMBUY
                                inner join CORP400D.GPIMI701.INPOP100 e on a.PURCH = e.PURCH
                                inner join CORP400D.GPIMI701.MSVMP550 f on a.VNDNO = f.VNDNO
                                where e.P1PAP = 'N' and a.PODYR = '25'
                              """;

            var val = await cmd.ExecuteScalarAsync();
            _testContext!.WriteLine($"Smoke query returned first scalar value: {val ?? "<null>"}");
            Assert.IsNotNull(val);
        }

        // =========================================================
        // Test 2: Strongly-typed end-to-end load (POCO approach)
        // =========================================================

        /// <summary>
        /// Loads candidate POs from INPUL500, then for each:
        /// - Loads PO Header from INPOL112
        /// - Loads PO Lines from INPOL300
        /// Materials data into POCOs and writes a short debug dump to test output.
        /// </summary>
        [TestMethod]
        public async Task Can_Load_POs_With_Header_And_Lines_StronglyTyped()
        {
            var candidates = new List<PoApprovalCandidate>();

            using var cn = new OdbcConnection(_cs);
            await cn.OpenAsync();
            _testContext!.WriteLine("ODBC connection opened successfully.");

            // 1) Seed read from INPUL500 (limit rows to keep unit test snappy)
            using (var cmd = cn.CreateCommand())
            {
                // NOTE:
                //  - P3CDTE is CYYMMDD (C=century 0/1, YY year, MM month, DD day)
                //  - P3CTIM is seconds since midnight (per your note "epoch")
                cmd.CommandText = @"
                    select distinct P3PURCH, P3DAMNT, P3IAMNT, P3CDTE, P3CTIM
                    from CORP400D.GPIMI701.INPUL500
                    fetch first 25 rows only
                ";

                using var rdr = await cmd.ExecuteReaderAsync();
                while (await rdr.ReadAsync())
                {
                    var purch = GetString(rdr, "P3PURCH") ?? "";
                    if (string.IsNullOrWhiteSpace(purch)) continue;

                    var cyymmdd = GetString(rdr, "P3CDTE");
                    var seconds = GetNullableDecimal(rdr, "P3CTIM");

                    candidates.Add(new PoApprovalCandidate
                    {
                        Purch = purch.Trim(),
                        DirectAmount = GetNullableDecimal(rdr, "P3DAMNT"),
                        IndirectAmount = GetNullableDecimal(rdr, "P3IAMNT"),
                        CreatedAt = ComposeDateTimeFromPrms(cyymmdd, seconds)
                    });
                }
            }

            _testContext!.WriteLine($"INPUL500 candidates loaded: {candidates.Count}");
            if (candidates.Count == 0)
            {
                _testContext!.WriteLine("No candidates found; exiting test early.");
                return; // do not fail the suite in data-scarce environments
            }

            // 2) For each candidate, fetch header + lines
            var results = new List<PurchaseOrder>();

            foreach (var c in candidates)
            {
                PoHeader? header = null;
                var lines = new List<PoLine>();

                // Header from INPOL112 (parameterized to avoid SQL injection and preserve plan reuse)
                using (var cmdH = cn.CreateCommand())
                {
                    cmdH.CommandText = "select * from CORP400D.GPIMI701.INPOL112 where PURCH = ?";
                    cmdH.Parameters.Add(new OdbcParameter("@purch", OdbcType.VarChar) { Value = c.Purch });

                    using var rdrH = await cmdH.ExecuteReaderAsync();
                    if (await rdrH.ReadAsync())
                    {
                        header = new PoHeader
                        {
                            Purch = GetString(rdrH, "PURCH") ?? c.Purch,
                            Buyer = GetStringIfExists(rdrH, "BUYER"),
                            VendorNo = GetStringIfExists(rdrH, "VNDNO"),
                            TotalCost = GetNullableDecimalIfExists(rdrH, "FOCST"),
                            ApprovalFlag = GetStringIfExists(rdrH, "P1PAP")
                        };
                    }
                }

                // Lines from INPOL300; note the quoted identifier "LINE#" due to the '#'
                using (var cmdL = cn.CreateCommand())
                {
                    cmdL.CommandText = @"
                        select *
                        from CORP400D.GPIMI701.INPOL300
                        where PURCH = ?
                        order by ""LINE#""
                    ";
                    cmdL.Parameters.Add(new OdbcParameter("@purch", OdbcType.VarChar) { Value = c.Purch });

                    using var rdrL = await cmdL.ExecuteReaderAsync();
                    while (await rdrL.ReadAsync())
                    {
                        lines.Add(new PoLine
                        {
                            Purch = GetString(rdrL, "PURCH") ?? c.Purch,
                            LineNo = GetNullableIntIfExists(rdrL, "LINE#"),
                            PartNo = GetStringIfExists(rdrL, "PRDNO"),
                            Qty = GetNullableDecimalIfExists(rdrL, "QUANO"),
                            UnitCost = GetNullableDecimalIfExists(rdrL, "ECOST"),
                            ExtCost = GetNullableDecimalIfExists(rdrL, "EXTCOST")
                                     ?? MultiplyNullable(GetNullableDecimalIfExists(rdrL, "QUANO"),
                                                         GetNullableDecimalIfExists(rdrL, "ECOST"))
                        });
                    }
                }

                if (header != null || lines.Count > 0)
                {
                    results.Add(new PurchaseOrder
                    {
                        Header = header ?? new PoHeader { Purch = c.Purch },
                        Lines = lines,
                        Source = c
                    });
                }
            }

            // 3) Basic sanity + debug dump to help you inspect the data
            Assert.IsTrue(results.Count > 0, "No PO headers/lines were materialized.");
            DumpPurchaseOrdersSummary(results, maxOrders: 5, maxLinesPerOrder: 3);
        }

        [TestMethod]
        public async Task Can_Open_SqlServer_And_Select1()
        {
            await using var cn = new SqlConnection(_sql);
            await cn.OpenAsync();

            await using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            var result = await cmd.ExecuteScalarAsync();
            Assert.AreEqual(1, Convert.ToInt32(result));
        }

        [TestMethod]
        public async Task Can_Run_Job_EndToEnd_Loads_Delegation_Of_Authority_Lookup_Tables()
        {
            // Arrange
            var prms = new PrmsReader(_cfg, _log);
            var options = new DbContextOptionsBuilder<ITTPortal.Infrastructure.PortalDbContext>().Options;
            using var db = new ITTPortal.Infrastructure.PortalDbContext(options, _dbCfg);

            // Same reader instance implements both interfaces (one method is explicit)
            IDelegationOfAuthorityDirectMaterialReader direct = prms;
            IDelegationOfAuthorityIndirectExpenseReader indirect = prms;

            var repo = new PoApprovalsDelegationOfAuthorityRepository(db, _log);
            var job = new FetchDelegationOfAuthorityJob(direct, indirect, repo, _log);

            // Act
            await job.RunAsync(CancellationToken.None);

            // Assert
            var directCount = await ExecScalarAsync<int>("SELECT COUNT(1) FROM dbo.PO_DelegationOfAuthority_Direct_Material");
            var indirectCount = await ExecScalarAsync<int>("SELECT COUNT(1) FROM dbo.PO_DelegationOfAuthority_Indirect_Expense");

            Assert.IsTrue(directCount >= 0, "Direct DOA load did not complete.");
            Assert.IsTrue(indirectCount >= 0, "Indirect DOA load did not complete.");

            // Optional spot checks
            var sampleDirectAmt = await ExecScalarAsync<decimal?>("SELECT TOP(1) Amount FROM dbo.PO_DelegationOfAuthority_Direct_Material");
            var sampleIndirectLvl = await ExecScalarAsync<string?>("SELECT TOP(1) [Level] FROM dbo.PO_DelegationOfAuthority_Indirect_Expense");

            // types & basic sanity
            _ = sampleDirectAmt;   // decimal(19,4)
            _ = sampleIndirectLvl; // nvarchar(50)
        }



        [TestMethod]
        public async Task Can_Run_Job_EndToEnd_Inserts_Into_SQL()
        {
            var prmsWriter = new PrmsWriter(_cfg, _log);
            var reader = new PrmsReader(_cfg, _log, prmsWriter);
            var options = new DbContextOptionsBuilder<PortalDbContext>().Options;
            using var db = new PortalDbContext(options, _dbCfg);
            var repo = new PoApprovalsStagingRepository(db, _log);
            var writer = new SqlWriter(repo, _log);
            var job = new FetchWaitingApprovalJob(reader, writer, _log);

            // 1) Run the job (reader + writer + dbo.PO_Merge)
            await job.RunAsync(CancellationToken.None);

            // 2) Staging must be empty (merge proc truncates on success)
            var stgH = await ExecScalarAsync<int>("SELECT COUNT(1) FROM dbo.PO_Stg_Header");
            var stgL = await ExecScalarAsync<int>("SELECT COUNT(1) FROM dbo.PO_Stg_Line");
            Assert.AreEqual(0, stgH, "dbo.PO_Stg_Header should be empty after merge.");
            Assert.AreEqual(0, stgL, "dbo.PO_Stg_Line should be empty after merge.");
        }

        //private async Task<int> CountHeadersAsync(IEnumerable<string> poNumbers)
        //{
        //    var list = poNumbers.ToList();
        //    var @in = string.Join(",", Enumerable.Range(0, list.Count).Select(i => $"@p{i}"));
        //    var sql = $"SELECT COUNT(*) FROM dbo.PO_Header WHERE PoNumber IN ({@in});";
        //    return await ExecScalarWithParamsAsync<int>(sql, list);
        //}

        //private async Task<int> CountLinesAsync(IEnumerable<string> poNumbers)
        //{
        //    var list = poNumbers.ToList();
        //    var @in = string.Join(",", Enumerable.Range(0, list.Count).Select(i => $"@p{i}"));
        //    var sql = $"SELECT COUNT(*) FROM dbo.PO_Line WHERE PoNumber IN ({@in});";
        //    return await ExecScalarWithParamsAsync<int>(sql, list);
        //}

        private async Task<T> ExecScalarAsync<T>(string sql)
        {
            await using var cn = new SqlConnection(_sql);
            await cn.OpenAsync();
            await using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;

            var o = await cmd.ExecuteScalarAsync();

            if (o is null || o is DBNull) return default!;

            var targetType = typeof(T);
            var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

            var converted = Convert.ChangeType(o, underlying, CultureInfo.InvariantCulture);
            return (T)converted!;
        }


        //private async Task<T> ExecScalarWithParamsAsync<T>(string sql, IList<string> values)
        //{
        //    await using var cn = new SqlConnection(_sql);
        //    await cn.OpenAsync();
        //    await using var cmd = cn.CreateCommand();
        //    cmd.CommandText = sql;
        //    for (int i = 0; i < values.Count; i++)
        //        cmd.Parameters.AddWithValue($"@p{i}", values[i]);
        //    var o = await cmd.ExecuteScalarAsync();
        //    return (T)Convert.ChangeType(o, typeof(T), CultureInfo.InvariantCulture)!;
        //}

        // =====================
        // Helpers (parsing, safe getters, dumping)
        // =====================

        /// <summary>
        /// Compose DateTime from PRMS date/time fields.
        /// P3CDTE is CYYMMDD (C=0 =&gt; 1900-based, C=1 =&gt; 2000-based by convention used here).
        /// P3CTIM is seconds since midnight (0..86399). Adjust if your system defines a different epoch.
        /// </summary>
        private static DateTime? ComposeDateTimeFromPrms(string? cyymmdd, decimal? secondsOfDay)
        {
            if (string.IsNullOrWhiteSpace(cyymmdd) || cyymmdd.Trim().Length != 7) return null;
            if (!int.TryParse(cyymmdd.Trim(), out var raw)) return null;

            var c = raw / 1_000_000;              // C
            var y = (raw / 10_000) % 100;         // YY
            var m = (raw / 100) % 100;            // MM
            var d = raw % 100;                    // DD

            // If your PRMS uses different century semantics, adjust here.
            var baseYear = c == 0 ? 1900 : 2000;
            var year = baseYear + y;

            try
            {
                var date = new DateTime(year, m, d);
                if (secondsOfDay is null) return date;

                var secs = (int)secondsOfDay.Value;
                if (secs < 0) secs = 0;
                if (secs > 86399) secs = 86399;

                return date.AddSeconds(secs);
            }
            catch
            {
                // Invalid calendar date (e.g., bad month/day) — return null gracefully.
                return null;
            }
        }

        /// <summary>Safe string getter (throws if column missing, null if DbNull).</summary>
        private static string? GetString(IDataRecord r, string name)
        {
            var ord = r.GetOrdinal(name);
            if (r.IsDBNull(ord)) return null;
            return r.GetValue(ord)?.ToString();
        }

        /// <summary>Safe string getter that first checks if a column exists.</summary>
        private static string? GetStringIfExists(IDataRecord r, string name)
        {
            var ord = TryGetOrdinal(r, name);
            if (ord < 0 || r.IsDBNull(ord)) return null;
            return r.GetValue(ord)?.ToString();
        }

        /// <summary>Safe decimal? getter (throws if column missing).</summary>
        private static decimal? GetNullableDecimal(IDataRecord r, string name)
        {
            var ord = r.GetOrdinal(name);
            if (r.IsDBNull(ord)) return null;
            return Convert.ToDecimal(r.GetValue(ord), CultureInfo.InvariantCulture);
        }

        /// <summary>Safe decimal? getter that first checks if a column exists.</summary>
        private static decimal? GetNullableDecimalIfExists(IDataRecord r, string name)
        {
            var ord = TryGetOrdinal(r, name);
            if (ord < 0 || r.IsDBNull(ord)) return null;
            return Convert.ToDecimal(r.GetValue(ord), CultureInfo.InvariantCulture);
        }

        /// <summary>Safe int? getter that first checks if a column exists.</summary>
        private static int? GetNullableIntIfExists(IDataRecord r, string name)
        {
            var ord = TryGetOrdinal(r, name);
            if (ord < 0 || r.IsDBNull(ord)) return null;
            return Convert.ToInt32(r.GetValue(ord), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Returns the ordinal for a column name (case-insensitive); -1 if not found.
        /// Useful to write robust tests when schemas vary slightly by environment.
        /// </summary>
        private static int TryGetOrdinal(IDataRecord r, string name)
        {
            for (int i = 0; i < r.FieldCount; i++)
                if (string.Equals(r.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return i;
            return -1;
        }

        /// <summary>Multiplies two nullable decimals if both have a value; otherwise returns null.</summary>
        private static decimal? MultiplyNullable(decimal? a, decimal? b)
            => (a.HasValue && b.HasValue) ? a.Value * b.Value : (decimal?)null;

        /// <summary>
        /// Writes a concise summary of the first N purchase orders and a few lines for each
        /// to the MSTest output (visible in Test Explorer). This is ideal for quick inspection.
        /// </summary>
        private static void DumpPurchaseOrdersSummary(List<PurchaseOrder> results, int maxOrders, int maxLinesPerOrder)
        {
            _testContext!.WriteLine($"--- Dumping up to {maxOrders} POs (with up to {maxLinesPerOrder} lines each) ---");
            int count = 0;

            foreach (var po in results)
            {
                if (count++ >= maxOrders) break;

                var h = po.Header;
                var s = po.Source;

                _testContext!.WriteLine(
                    $"PO {h.Purch} | Buyer={h.Buyer ?? "<null>"} | Vendor={h.VendorNo ?? "<null>"} | " +
                    $"TotalCost={h.TotalCost?.ToString("0.00") ?? "<null>"} | Approval={h.ApprovalFlag ?? "<null>"} | " +
                    $"Seed: Direct={s.DirectAmount?.ToString("0.00") ?? "<null>"} Indirect={s.IndirectAmount?.ToString("0.00") ?? "<null>"} " +
                    $"CreatedAt={s.CreatedAt?.ToString("yyyy-MM-dd HH:mm:ss") ?? "<null>"} | Lines={po.Lines.Count}"
                );

                int lc = 0;
                foreach (var line in po.Lines)
                {
                    if (lc++ >= maxLinesPerOrder) break;
                    _testContext!.WriteLine(
                        $"   Line {line.LineNo?.ToString() ?? "?"} | Part={line.PartNo ?? "<null>"} | " +
                        $"Qty={line.Qty?.ToString("0.####") ?? "<null>"} | " +
                        $"Unit={line.UnitCost?.ToString("0.####") ?? "<null>"} | " +
                        $"Ext={line.ExtCost?.ToString("0.####") ?? "<null>"}"
                    );
                }
            }
            _testContext!.WriteLine("--- End dump ---");
        }
    }

    [TestClass]
    public class MappingTests
    {
        [TestMethod]
        public void StatusMappings_Are_Correct()
        {
            Assert.AreEqual('Y', StatusMaps.ToPrmsP3(Decision.Approve));
            Assert.AreEqual('N', StatusMaps.ToPrmsP3(Decision.Deny));
            Assert.AreEqual('A', StatusMaps.ToPrmsP5(Decision.Approve));
            Assert.AreEqual('R', StatusMaps.ToPrmsP5(Decision.Deny));
        }
    }
}
