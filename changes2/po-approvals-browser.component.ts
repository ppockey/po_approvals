import { Component, ViewChild } from '@angular/core';
import { HttpClient, HttpErrorResponse, HttpParams } from '@angular/common/http';
import { DialogComponent } from '@syncfusion/ej2-angular-popups';
import {
  CommandModel,
  CommandClickEventArgs,
} from '@syncfusion/ej2-angular-grids';
import {
  PoDetailResponse,
  PoHeaderView,
  PoLineView,
  PoListResponse,
  PoStage,
} from 'src/app/core/models/poapprovals/poapprovals_models';

@Component({
  selector: 'app-po-approvals-browser',
  templateUrl: './po-approvals-browser.component.html',
  styleUrls: ['./po-approvals-browser.component.css'],
})
export class PoApprovalsBrowserComponent {
  // ────────────────────── Public state (template-bound) ──────────────────────
  public poRows: PoHeaderView[] = [];

  public detailHeader: PoHeaderView | null = null;
  public lines: PoLineView[] = [];
  public stages: PoStage[] = [];
  public dialogOpen = false;

  // Command column buttons (used by template)
  public commands: CommandModel[] = [
    {
      type: 'None',
      buttonOption: { content: 'Details', cssClass: 'e-primary cmd-view' },
    },
    {
      type: 'None',
      buttonOption: {
        content: 'Documentation',
        cssClass: 'e-info cmd-docs ml',
      },
    },
  ];

  // Docs dialog state (template reads these)
  public currentPoForDocs: PoHeaderView | null = null;
  public notesText: string = '';
  public selectedFiles: File[] = [];

  // ────────────────────── Private template refs ──────────────────────────────
  @ViewChild('poDialog', { static: false })
  private poDialog?: DialogComponent;
  @ViewChild('docDialog', { static: false })
  private docDialog?: DialogComponent;

  // ────────────────────── Construction & lifecycle ───────────────────────────
  public constructor(private http: HttpClient) {}

  private userEmail = `${
    JSON.parse(localStorage.getItem('currentUser')).networkUsername
  }@itt.com`;
  public ngOnInit(): void {
    const params = new HttpParams()
      .set('page', '1')
      .set('pageSize', '20')
      .set('email', this.userEmail);
    this.http.get<PoListResponse>('/api/po', { params }).subscribe((resp) => {
      const rows = (resp?.rows ?? []).map(this.toHeader);
      this.poRows = rows;
    });
  }

  public onCommandClick(args: CommandClickEventArgs): void {
    const row: any = args.rowData;
    const target = args.target as HTMLElement | null;
    const poNumber: string = row?.poNumber ?? row?.PoNumber ?? '';

    if (target?.classList.contains('cmd-view')) {
      this.loadDetails(poNumber, row);
      return;
    }
    if (target?.classList.contains('cmd-docs')) {
      this.openDocsForRow(row);
      return;
    }

    // Fallback (labels differ from current button text; only used if classes change)
    // const label = (args.commandColumn as any)?.buttonOption?.content as
    //   | string
    //   | undefined;
    // if (label === 'Details') this.loadDetails(poNumber, row);
    // else if (label === 'Documentation') this.openDocsForRow(row);
  }

  public onDlgOpen(): void {
    this.dialogOpen = true;
    this.freezePageScroll();
  }

  public onDlgClose(): void {
    this.dialogOpen = false;
    this.unfreezePageScroll();
  }

  public openDocsForRow(row: any): void {
    this.currentPoForDocs = this.toHeader(row ?? {});
    this.notesText = '';
    this.selectedFiles = [];
    this.docDialog?.show();
    requestAnimationFrame(() => this.docDialog?.refreshPosition());
  }

  public onDocDlgOpen(): void {
    this.onDlgOpen(); // reuse scroll lock
    this.docDialog?.refreshPosition();
  }

  public onDocDlgClose(): void {
    this.onDlgClose(); // reuse scroll unlock
    this.currentPoForDocs = null;
    this.notesText = '';
    this.selectedFiles = [];
  }

  public onNotesInput(value: string): void {
    this.notesText = value;
  }

  public onFilesSelected(args: any): void {
    const list: File[] = Array.from(
      (args?.event?.target?.files ?? []) as FileList
    );
    this.selectedFiles = list;
  }

  public removeSelectedFile(idx: number): void {
    this.selectedFiles.splice(idx, 1);
    this.selectedFiles = [...this.selectedFiles];
  }

  public saveNotes(): void {
    console.log('saveNotes() not implemented', {
      poNumber: this.currentPoForDocs?.poNumber,
      notes: this.notesText,
    });
    alert('Notes saved (stub). Implement API call.');
  }

  public uploadSelectedFiles(): void {
    console.log('uploadSelectedFiles() not implemented', {
      poNumber: this.currentPoForDocs?.poNumber,
      files: this.selectedFiles,
    });
    alert('Files upload (stub). Implement API call.');
  }

  // public approve(): void {
  //   if (!this.detailHeader) return;
  //   const seq = this.findMyPendingSequence();
  //   if (seq == null) {
  //     alert('No pending stage for you on this PO.');
  //     return;
  //   }

  //   const url = `/api/po/${encodeURIComponent(
  //     this.detailHeader.poNumber
  //   )}/stages/${seq}/approve`;
  //   this.http
  //     .post(url, { userId: this.userEmail, note: this.notesText })
  //     .subscribe({
  //       next: () => {
  //         // Refresh detail state
  //         this.loadDetails(this.detailHeader!.poNumber);
  //       },
  //       error: (err) =>
  //         alert(`Approve failed: ${err?.error ?? err?.message ?? err}`),
  //     });
  // }

  // public deny(): void {
  //   if (!this.detailHeader) return;
  //   const seq = this.findMyPendingSequence();
  //   if (seq == null) {
  //     alert('No pending stage for you on this PO.');
  //     return;
  //   }

  //   const url = `/api/po/${encodeURIComponent(
  //     this.detailHeader.poNumber
  //   )}/stages/${seq}/deny`;
  //   this.http
  //     .post(url, { userId: this.userEmail, note: this.notesText })
  //     .subscribe({
  //       next: () => {
  //         this.loadDetails(this.detailHeader!.poNumber);
  //       },
  //       error: (err) =>
  //         alert(`Deny failed: ${err?.error ?? err?.message ?? err}`),
  //     });
  // }
  private showHttpError(err: any): void {
    // Try several common shapes (ProblemDetails, plain text, ASP.NET default, custom)
    let msg = 'Unexpected error.';
    if (err instanceof HttpErrorResponse) {
      const e = err;
      // 1) ASP.NET Core ProblemDetails-like { title, detail, status, path }
      const pd = (e.error || {}) as any;
      if (pd && (pd.title || pd.detail)) {
        msg = `${pd.title ?? 'Error'}${pd.detail ? `: ${pd.detail}` : ''}`;
      }
      // 2) Server sent a string
      else if (typeof e.error === 'string') {
        msg = e.error;
      }
      // 3) Fallback to status + message
      else if (e.message) {
        msg = `${e.status || ''} ${e.statusText || ''} ${e.message}`.trim();
      }
    } else if (typeof err === 'string') {
      msg = err;
    } else if (err && err.message) {
      msg = err.message;
    }
    alert(msg);
  }

  public approve(): void {
    if (!this.detailHeader) return;
    const seq = this.findMyPendingSequence();
    if (seq == null) {
      alert('No pending stage for you on this PO.');
      return;
    }
    const url = `/api/po/${encodeURIComponent(
      this.detailHeader.poNumber
    )}/stages/${seq}/approve`;
    this.http
      .post(url, { userId: this.userEmail, note: this.notesText })
      .subscribe({
        next: () => {
          this.loadDetails(this.detailHeader!.poNumber);
          // Optionally refresh the queue:
          // this.ngOnInit();
        },
        error: (err) => this.showHttpError(err),
      });
  }

  public deny(): void {
    if (!this.detailHeader) return;
    const seq = this.findMyPendingSequence();
    if (seq == null) {
      alert('No pending stage for you on this PO.');
      return;
    }
    const url = `/api/po/${encodeURIComponent(
      this.detailHeader.poNumber
    )}/stages/${seq}/deny`;
    this.http
      .post(url, { userId: this.userEmail, note: this.notesText })
      .subscribe({
        next: () => {
          this.loadDetails(this.detailHeader!.poNumber);
        },
        error: (err) => this.showHttpError(err),
      });
  }

  // ────────────────────── Private helpers: mapping & API ─────────────────────
  private toHeader = (h: any): PoHeaderView => ({
    poNumber: h.poNumber ?? h.PoNumber ?? '',
    poDate: h.poDate ?? h.PoDate,
    vendorName: h.vendorName ?? h.VendorName,
    buyerName: h.buyerName ?? h.BuyerName,
    houseCode: h.houseCode ?? h.HouseCode,
    directAmount: h.directAmount ?? h.DirectAmount ?? 0,
    indirectAmount: h.indirectAmount ?? h.IndirectAmount ?? 0,
    totalAmount:
      h.totalAmount ??
      h.TotalAmount ??
      (h.directAmount ?? h.DirectAmount ?? 0) +
        (h.indirectAmount ?? h.IndirectAmount ?? 0),
    status: (h.status ?? h.Status ?? 'W') as 'W' | 'A' | 'D',
    isActive: (h.isActive ?? h.IsActive ?? true) as boolean,
    createdAtUtc: h.createdAtUtc ?? h.CreatedAtUtc,
    activeLineCount: h.activeLineCount ?? h.ActiveLineCount ?? 0,
  });

  private toLine = (l: any): PoLineView => ({
    poNumber: l.poNumber ?? l.PoNumber ?? '',
    lineNumber: l.lineNumber ?? l.LineNumber ?? 0,
    itemNumber: l.itemNumber ?? l.ItemNumber ?? '',
    itemDescription: l.itemDescription ?? l.ItemDescription,
    specialDescription: l.specialDescription ?? l.SpecialDescription,
    quantityOrdered: l.quantityOrdered ?? l.QuantityOrdered,
    orderUom: l.orderUom ?? l.OrderUom,
    unitCost: l.unitCost ?? l.UnitCost,
    extendedCost: l.extendedCost ?? l.ExtendedCost,
    requiredDate: l.requiredDate ?? l.RequiredDate,
    glAccount: l.glAccount ?? l.GlAccount,
    isActive: (l.isActive ?? l.IsActive ?? true) as boolean,
  });

  private toStage = (s: any): PoStage => ({
    poNumber: s.poNumber ?? s.PoNumber ?? '',
    sequence: s.sequence ?? s.Sequence ?? 0,
    roleCode: s.roleCode ?? s.RoleCode ?? '',
    approverUserId: s.approverUserId ?? s.ApproverUserId,
    category: (s.category ?? s.Category ?? null) as 'I' | 'D' | null,
    thresholdFrom: s.thresholdFrom ?? s.ThresholdFrom,
    thresholdTo: s.thresholdTo ?? s.ThresholdTo,
    status: (s.status ?? s.Status ?? 'P') as 'P' | 'A' | 'D' | 'S',
    decidedAtUtc: s.decidedAtUtc ?? s.DecidedAtUtc,
  });

  private loadDetails(poNumber: string, fallbackRow?: any): void {
    if (!poNumber) return;
    this.http
      .get<PoDetailResponse>(`/api/po/${encodeURIComponent(poNumber)}`)
      .subscribe((d) => {
        this.detailHeader = this.toHeader(d?.header ?? fallbackRow ?? {});
        this.lines = (d?.lines ?? []).map(this.toLine);
        this.stages = (d?.stages ?? []).map(this.toStage);
        this.poDialog?.show();
      });
  }

  // ────────────────────── Helpers Approve/Deny ───────────────────────
  public findMyPendingSequence(): number | null {
    const me = this.userEmail?.toLowerCase();
    const s = this.stages.find(
      (x) => x.status === 'P' && (x.approverUserId ?? '').toLowerCase() === me
    );
    return s ? s.sequence : null;
  }

  private getNextPendingStage(): PoStage | null {
    return (this.stages ?? []).find((s) => s.status === 'P') ?? null;
  }

  // private isUserNextApprover(): boolean {
  //   const next = this.getNextPendingStage();
  //   if (!next) return false;
  //   const me = this.userEmail?.toLowerCase();
  //   return (next.approverUserId ?? '').toLowerCase() === (me ?? '');
  // }

  // private postDecision(
  //   endpoint: 'approve' | 'deny',
  //   sequence: number,
  //   note?: string
  // ): void {
  //   const po = this.detailHeader?.poNumber;
  //   if (!po || !sequence) return;

  //   const params = new HttpParams().set('email', this.userEmail);

  //   this.http
  //     .post<void>(
  //       `/api/po/${encodeURIComponent(po)}/${endpoint}`,
  //       { sequence, note },
  //       { params }
  //     )
  //     .subscribe({
  //       next: () => {
  //         this.loadDetails(po, this.detailHeader);
  //         // Optionally refresh queue if finalized
  //         this.ngOnInit();
  //       },
  //       error: (err) => {
  //         if (err?.status === 409) {
  //           alert(
  //             'This PO is no longer pending or someone else already acted. Refreshing…'
  //           );
  //           this.loadDetails(po, this.detailHeader);
  //           this.ngOnInit();
  //           return;
  //         }
  //         alert('Action failed. Please try again.');
  //       },
  //     });
  // }

  // ────────────────────── Private: page scroll locking ───────────────────────
  private scrollY = 0;
  private bodyStyleBackup: Partial<CSSStyleDeclaration> = {};
  private lockDepth = 0;

  private freezePageScroll(): void {
    if (this.lockDepth++ > 0) return;
    if (typeof window === 'undefined' || typeof document === 'undefined')
      return;

    this.scrollY = window.scrollY || window.pageYOffset || 0;
    const b = document.body;
    this.bodyStyleBackup = {
      position: b.style.position,
      overflow: b.style.overflow,
      width: b.style.width,
      top: b.style.top,
      left: b.style.left,
      right: b.style.right,
    };
    b.style.position = 'fixed';
    b.style.overflow = 'hidden';
    b.style.width = '100%';
    b.style.top = `-${this.scrollY}px`;
    b.style.left = '0';
    b.style.right = '0';
  }

  private unfreezePageScroll(): void {
    if (--this.lockDepth > 0) return;
    if (typeof window === 'undefined' || typeof document === 'undefined')
      return;

    const b = document.body;
    b.style.position = this.bodyStyleBackup.position ?? '';
    b.style.overflow = this.bodyStyleBackup.overflow ?? '';
    b.style.width = this.bodyStyleBackup.width ?? '';
    b.style.top = this.bodyStyleBackup.top ?? '';
    b.style.left = this.bodyStyleBackup.left ?? '';
    b.style.right = this.bodyStyleBackup.right ?? '';
    window.scrollTo(0, this.scrollY);
  }
}
