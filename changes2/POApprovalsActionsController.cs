using ITTPortal.POApprovals.Services;
using Microsoft.AspNetCore.Mvc;
using System.Threading;
using System.Threading.Tasks;

namespace ITTPortal.Web.Controllers
{
    [ApiController]
    [Route("api/po/{poNumber}/stages/{sequence:int}")]
    public sealed class POApprovalsActionsController : BaseApiController
    {
        private readonly PoApprovalsService _svc;
        public POApprovalsActionsController(PoApprovalsService svc) => _svc = svc;

        public sealed record DecideDto(string userId, string? note);

        [HttpPost("approve")]
        public async Task<IActionResult> Approve(
            [FromRoute] string poNumber,
            [FromRoute] int sequence,
            [FromBody] DecideDto dto,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(dto?.userId)) return BadRequest("userId is required");
            await _svc.ApproveAsync(poNumber, sequence, dto.userId, dto.note, ct);
            return NoContent();
        }

        [HttpPost("deny")]
        public async Task<IActionResult> Deny(
            [FromRoute] string poNumber,
            [FromRoute] int sequence,
            [FromBody] DecideDto dto,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(dto?.userId)) return BadRequest("userId is required");
            await _svc.DenyAsync(poNumber, sequence, dto.userId, dto.note, ct);
            return NoContent();
        }
    }
}
