import { Component, ViewChild } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
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
import { forkJoin, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';

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

  // ▼▼ Set these for development filtering ▼▼
  private readonly userEmail = 'so-gbl-ppockey@itt.com';
  private readonly userRoleCode = 'FINANCIAL CONTROLLER (SITE)';

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
  @ViewChild('poDialog', { static: false }) private poDialog?: DialogComponent;
  @ViewChild('docDialog', { static: false })
  private docDialog?: DialogComponent;

  // ────────────────────── Construction & lifecycle ───────────────────────────
  public constructor(private http: HttpClient) {}

  public ngOnInit(): void {
    const params = new HttpParams().set('page', '1').set('pageSize', '20');

    // 1) Get the page of headers
    this.http
      .get<PoListResponse>('/api/po', { params })
      .pipe(
        map((resp) => (resp?.rows ?? []).map(this.toHeader)),
        // 2) For each header, fetch details to inspect stages and filter by (email, roleCode, pending)
        switchMap((headers) => {
          if (!headers.length) return of([] as PoHeaderView[]);
          const detailCalls = headers.map((h) =>
            this.http
              .get<PoDetailResponse>(`/api/po/${encodeURIComponent(h.poNumber)}`)
              .pipe(
                map((d) => {
                  const stages = (d?.stages ?? []).map(this.toStage);
                  const matches = stages.some(
                    (s) =>
                      s.status === 'P' &&
                      (s.approverUserId ?? '').toLowerCase() ===
                        this.userEmail.toLowerCase() &&
                      (s.roleCode ?? '').toLowerCase() ===
                        this.userRoleCode.toLowerCase()
                  );
                  return matches ? this.toHeader(d?.header ?? h) : null;
                }),
                catchError(() => of(null)) // if one detail call fails, just exclude that PO
              )
          );
          return forkJoin(detailCalls).pipe(
            map((list) => list.filter((x): x is PoHeaderView => !!x))
          );
        })
      )
      .subscribe((filtered) => {
        this.poRows = filtered;
      });
  }

  // ────────────────────── Public handlers: grid / dialogs / inputs ───────────
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

    // Fallback
    const label = (args.commandColumn as any)?.buttonOption?.content as
      | string
      | undefined;
    if (label === 'Details') this.loadDetails(poNumber, row);
    else if (label === 'Documentation') this.openDocsForRow(row);
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
    this.onDlgOpen();
    this.docDialog?.refreshPosition();
  }

  public onDocDlgClose(): void {
    this.onDlgClose();
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

  public approve(): void {
    alert('You clicked the Approved button.');
  }
  public deny(): void {
    alert('You clicked the Deny button.');
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
    approverUserId: s.approverUserId ?? s.ApproverUserId, // this is the email in your current backend
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
