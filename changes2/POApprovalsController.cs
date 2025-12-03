using ITTPortal.POApprovals.Abstraction;
using Microsoft.AspNetCore.Mvc;
using System.Threading;
using System.Threading.Tasks;

namespace ITTPortal.Web.Controllers
{
    [ApiController]
    [Route("api/po")]
    public sealed class POApprovalsController : BaseApiController
    {
        private readonly IPurchaseOrderQueryService _poService;
        public POApprovalsController(IPurchaseOrderQueryService poService) => _poService = poService;

        [HttpGet]
        public async Task<IActionResult> List(
            [FromQuery] int page = 1,
            [FromQuery] int pageSize = 20,
            [FromQuery] string? email = null,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(email))
                return BadRequest("Query parameter 'email' is required");

            var result = await _poService.GetUserQueueAsync(email!, page, pageSize, ct);
            return Ok(result);
        }

        [HttpGet("{poNumber}")]
        public async Task<IActionResult> Detail([FromRoute] string poNumber, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(poNumber))
                return BadRequest("poNumber is required.");

            var detail = await _poService.GetDetailAsync(poNumber, ct);
            if (detail is null) return NotFound();

            return Ok(detail);
        }
    }
}