// src/app/po-approvals/pages/po-list/component.ts
import { Component, OnInit, OnDestroy, HostListener, ChangeDetectorRef, ViewChild, ElementRef } from '@angular/core';
import { PoService } from '../../services/po-service';
import { PoHeaderView, PoDetailResponse, PoStage } from '../../models/po-models';
import { buildMockStages } from '../../utils/mock-stages';

type DetailState =
  | { loading: true; error?: undefined; data?: undefined }
  | { loading: false; error?: string; data?: undefined }
  | { loading: false; error?: undefined; data: PoDetailResponse };

@Component({
  selector: 'app-po-approvals-po-list',
  templateUrl: './component.html',
  styleUrls: ['./component.css'],
})
export class PoApprovalsPoListComponent implements OnInit, OnDestroy {
  // List state
  rows: PoHeaderView[] = [];
  total = 0;
  page = 1;
  pageSize = 20;
  search = '';
  loading = false;
  error?: string;

  // Expand/collapse state per PO
  private expanded = new Set<string>();
  private detail = new Map<string, DetailState>();

  // Modal state
  modalOpen = false;
  modalLoading = false;
  modalError?: string;
  modalData?: PoDetailResponse;

  // Demo placeholders (non-functional)
  draftNote = '';
  draftFiles: File[] = [];

  // Cache mock stages per PO
  private mockStagesByPo = new Map<string, PoStage[]>();

  // <input type="file"> reference
  @ViewChild('fileInput', { static: false }) fileInput?: ElementRef<HTMLInputElement>;

  // for restoring scroll (hard lock)
  private _scrollY = 0;

  constructor(private po: PoService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.load();
  }

  ngOnDestroy(): void {
    document.body.classList.remove('po-modal-open');
    // restore if ever left mid-lock
    if (document.body.style.position === 'fixed') {
      const y = parseInt(document.body.style.top || '0', 10) || 0;
      document.body.style.position = '';
      document.body.style.top = '';
      document.body.style.left = '';
      document.body.style.right = '';
      document.body.style.width = '';
      window.scrollTo(0, -y);
    }
  }

  @HostListener('window:keydown', ['$event'])
  onKeydown(e: KeyboardEvent) {
    if (this.modalOpen && e.key === 'Escape') this.closeModal();
  }

  // ---------- List + paging ----------
  load(): void {
    this.loading = true;
    this.error = undefined;

    this.po.list(this.page, this.pageSize, this.search).subscribe({
      next: (res) => {
        this.rows = res.rows;
        this.total = res.total;
        this.loading = false;
      },
      error: () => {
        this.error = 'Failed to load POs';
        this.loading = false;
      },
    });
  }

  onSearch(): void {
    this.page = 1;
    this.load();
  }

  totalPages(): number {
    if (this.pageSize <= 0) return 1;
    return Math.max(1, Math.ceil(this.total / this.pageSize));
  }

  prev(): void {
    if (this.page > 1) {
      this.page--;
      this.load();
    }
  }

  next(): void {
    if (this.page * this.pageSize < this.total) {
      this.page++;
      this.load();
    }
  }

  trackByPo = (_: number, r: PoHeaderView) => r.poNumber;

  // ---------- Row expand/preview ----------
  isExpanded(poNumber: string): boolean {
    return this.expanded.has(poNumber);
  }

  state(poNumber: string): DetailState | undefined {
    return this.detail.get(poNumber);
  }

  private fetchDetail(poNumber: string): void {
    this.detail.set(poNumber, { loading: true });
    this.po.get(poNumber).subscribe({
      next: (res) => this.detail.set(poNumber, { loading: false, data: res }),
      error: () =>
        this.detail.set(poNumber, { loading: false, error: 'Failed to load details' }),
    });
  }

  toggle(poNumber: string): void {
    if (this.isExpanded(poNumber)) {
      this.expanded.delete(poNumber);
      return;
    }
    this.expanded.add(poNumber);
    const st = this.detail.get(poNumber);
    if (!st || ('error' in st && !!st.error)) this.fetchDetail(poNumber);
  }

  // ---------- Modal (+ mock stages) ----------
  openModal(poNumber: string, ev?: MouseEvent): void {
    ev?.preventDefault();
    ev?.stopPropagation();

    document.documentElement.classList.add('po-modal-open');
    document.body.classList.add('po-modal-open');

    // Hard lock the page
    this._scrollY = window.scrollY || window.pageYOffset || 0;
    document.body.style.position = 'fixed';
    document.body.style.top = `-${this._scrollY}px`;
    document.body.style.left = '0';
    document.body.style.right = '0';
    document.body.style.width = '100%';

    this.modalOpen = true;
    this.modalLoading = true;
    this.modalError = undefined;
    this.modalData = undefined;
    this.draftNote = '';
    this.draftFiles = [];
    this.cdr.detectChanges();

    this.po.get(poNumber).subscribe({
      next: (res) => {
        this.modalData = res;
        this.modalLoading = false;
        this.cdr.detectChanges();
      },
      error: () => {
        this.modalError = 'Failed to load details';
        this.modalLoading = false;
        this.cdr.detectChanges();
      },
    });
  }

  closeModal(): void {
    this.modalOpen = false;

    // Restore body scroll
    const y = parseInt(document.body.style.top || '0', 10) || 0;
    document.body.style.position = '';
    document.body.style.top = '';
    document.body.style.left = '';
    document.body.style.right = '';
    document.body.style.width = '';
    window.scrollTo(0, -y);

    document.documentElement.classList.remove('po-modal-open');
    document.body.classList.remove('po-modal-open');

    this.cdr.detectChanges();
  }

  /** Stages for the modal: prefer real API stages, otherwise mock */
  get modalStages(): PoStage[] {
    if (!this.modalData) return [];
    const real = this.modalData.stages ?? [];
    if (real.length > 0) return real;

    const key = this.modalData.header.poNumber;
    if (!this.mockStagesByPo.has(key)) {
      this.mockStagesByPo.set(key, buildMockStages(key, this.modalData.header.totalAmount));
    }
    return this.mockStagesByPo.get(key)!;
  }

  // ---------- Files (demo) ----------
  bytesToHuman(bytes: number): string {
    if (!bytes && bytes !== 0) return '';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let i = 0, n = bytes;
    while (n >= 1024 && i < units.length - 1) {
      n /= 1024; i++;
    }
    return `${n.toFixed(n >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
  }

  get totalSelectedSize(): string {
    const sum = this.draftFiles.reduce((s, f) => s + (f.size || 0), 0);
    return this.bytesToHuman(sum);
  }

  onFilesSelected(evt: Event): void {
    const input = evt.target as HTMLInputElement;
    const picked = input?.files ? Array.from(input.files) : [];
    // Merge with existing
    this.draftFiles = [...this.draftFiles, ...picked];

    // Reset input so same files can be picked again if needed
    if (this.fileInput?.nativeElement) {
      this.fileInput.nativeElement.value = '';
    } else if (input) {
      input.value = '';
    }

    this.cdr.detectChanges();
  }

  removeFile(index: number, ev?: Event): void {
    ev?.preventDefault();
    ev?.stopPropagation();

    // Create a new array to ensure change detection
    this.draftFiles = [
      ...this.draftFiles.slice(0, index),
      ...this.draftFiles.slice(index + 1),
    ];

    this.cdr.detectChanges();
  }

  clearFiles(ev?: Event): void {
    ev?.preventDefault();
    ev?.stopPropagation();

    this.draftFiles = [];

    if (this.fileInput?.nativeElement) {
      this.fileInput.nativeElement.value = '';
    } else {
      const el = document.getElementById('files') as HTMLInputElement | null;
      if (el) el.value = '';
    }

    this.cdr.detectChanges();
  }
}
