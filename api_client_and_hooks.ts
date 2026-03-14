/**
 * sourceiq/src/lib/api-client.ts
 * Typed fetch wrapper for all /api/v1/* endpoints.
 * All requests go through this client — never fetch() directly in components.
 */

export type ApiResponse<T> = {
  success: boolean
  data: T | null
  meta: ApiMeta | null
  error: ApiError | null
}

export type ApiMeta = {
  total?: number
  page?: number
  per_page?: number
  total_pages?: number
  reconciled_rows?: number
  unreconciled_rows?: number
  reconciliation_rate_pct?: number
  data_sources?: string[]
  last_refreshed_at?: string | null
}

export type ApiError = {
  code: string
  message: string
  fields?: Record<string, string>
  correlation_id?: string
  context?: Record<string, unknown>
}

class ApiClient {
  private baseUrl = '/api/v1'
  private token: string | null = null

  setToken(token: string | null) { this.token = token }

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
    params?: Record<string, string | number | boolean | undefined>
  ): Promise<ApiResponse<T>> {
    const url = new URL(`${this.baseUrl}${path}`, window.location.origin)
    if (params) {
      Object.entries(params).forEach(([k, v]) => {
        if (v !== undefined && v !== null) url.searchParams.set(k, String(v))
      })
    }

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    }
    if (this.token) headers['Authorization'] = `Bearer ${this.token}`

    const resp = await fetch(url.toString(), {
      method,
      headers,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    })

    // 401 → clear session + redirect to login
    if (resp.status === 401) {
      this.token = null
      window.location.href = '/login?expired=1'
      return { success: false, data: null, meta: null, error: { code: 'UNAUTHORIZED', message: 'Sessão expirada.' } }
    }

    const json = await resp.json()
    return json as ApiResponse<T>
  }

  get<T>(path: string, params?: Record<string, string | number | boolean | undefined>) {
    return this.request<T>('GET', path, undefined, params)
  }
  post<T>(path: string, body?: unknown) { return this.request<T>('POST', path, body) }
  patch<T>(path: string, body?: unknown) { return this.request<T>('PATCH', path, body) }
  del<T>(path: string) { return this.request<T>('DELETE', path) }

  // Multipart upload (attachments, imports)
  async upload<T>(path: string, form: FormData): Promise<ApiResponse<T>> {
    const headers: Record<string, string> = {}
    if (this.token) headers['Authorization'] = `Bearer ${this.token}`
    const resp = await fetch(`${this.baseUrl}${path}`, { method: 'POST', headers, body: form })
    if (resp.status === 401) {
      this.token = null
      window.location.href = '/login?expired=1'
      return { success: false, data: null, meta: null, error: { code: 'UNAUTHORIZED', message: '' } }
    }
    return resp.json()
  }
}

export const apiClient = new ApiClient()

// ─── TYPED API METHODS ────────────────────────────────────────────────────────

export const api = {
  // Auth
  login:   (email: string, password: string) =>
    apiClient.post<{ user: UserProfile; access_token: string; expires_at: string }>
    ('/auth/login', { email, password }),
  me:      () => apiClient.get<UserProfile>('/auth/me'),
  logout:  () => apiClient.post('/auth/logout'),

  // Lots
  getLots: (params?: LotListParams) => apiClient.get<Lot[]>('/lots', params as any),
  getLot:  (id: string) => apiClient.get<LotDetail>(`/lots/${id}`),
  createLot: (body: CreateLotBody) => apiClient.post<Lot>('/lots', body),
  addMovement: (lotId: string, body: CreateMovementBody) =>
    apiClient.post<LotMovement>(`/lots/${lotId}/movements`, body),

  // Inspections
  getInspections: (params?: InspectionListParams) =>
    apiClient.get<Inspection[]>('/inspections', params as any),
  createInspection: (body: CreateInspectionBody) =>
    apiClient.post<Inspection>('/inspections', body),
  getInspection: (id: string) => apiClient.get<InspectionDetail>(`/inspections/${id}`),
  addDefectItem: (inspectionId: string, body: CreateDefectItemBody) =>
    apiClient.post<DefectItem>(`/inspections/${inspectionId}/items`, body),
  completeInspection: (id: string, body: { notes?: string }) =>
    apiClient.patch<Inspection>(`/inspections/${id}/complete`, body),

  // Non-conformities
  getNCs: (params?: NCListParams) => apiClient.get<NC[]>('/non-conformities', params as any),
  createNC: (body: CreateNCBody) => apiClient.post<NC>('/non-conformities', body),
  updateNC: (id: string, body: UpdateNCBody) => apiClient.patch<NC>(`/non-conformities/${id}`, body),
  getActionPlans: (ncId: string) => apiClient.get<ActionPlan[]>(`/non-conformities/${ncId}/action-plans`),
  createActionPlan: (ncId: string, body: CreateActionPlanBody) =>
    apiClient.post<ActionPlan>(`/non-conformities/${ncId}/action-plans`, body),

  // Imports
  uploadFile: (form: FormData) => apiClient.upload<ImportFile>('/imports/files', form),
  parseFile: (id: string) => apiClient.post<{ job_id: string }>(`/imports/files/${id}/parse`),
  getImportFile: (id: string) => apiClient.get<ImportFile>(`/imports/files/${id}`),
  getImportSummary: (id: string) => apiClient.get<ImportSummary>(`/imports/files/${id}/summary`),
  getStagingRows: (id: string, params?: { validation_status?: string; page?: number }) =>
    apiClient.get<StagingRow[]>(`/imports/files/${id}/staging`, params as any),
  validateFile: (id: string) => apiClient.post<ImportFile>(`/imports/files/${id}/validate`),
  commitFile: (id: string, force = false) =>
    apiClient.post<ImportFile>(`/imports/files/${id}/commit`, { force }),
  getImportLogs: (fileId: string, params?: { level?: string; page?: number }) =>
    apiClient.get<ImportLog[]>(`/imports/logs/${fileId}`, params as any),

  // Reconciliation
  createRun: (body: CreateRunBody) => apiClient.post<ReconciliationRun>('/reconciliation/runs', body),
  executeRun: (id: string) => apiClient.post<ReconciliationRun>(`/reconciliation/runs/${id}/execute`),
  getRuns: (params?: { status?: string; run_type?: string }) =>
    apiClient.get<ReconciliationRun[]>('/reconciliation/runs', params as any),
  getRun: (id: string) => apiClient.get<ReconciliationRun>(`/reconciliation/runs/${id}`),
  getRunItems: (id: string, params?: { comparison_status?: string; is_resolved?: boolean }) =>
    apiClient.get<ReconciliationItem[]>(`/reconciliation/runs/${id}/items`, params as any),
  resolveItem: (itemId: string, body: ResolveItemBody) =>
    apiClient.post<ReconciliationException>(`/reconciliation/items/${itemId}/resolve`, body),

  // Dashboards
  dashboard: {
    executive:  () => apiClient.get<ExecutiveOverview>('/dashboard/executive/overview'),
    alerts:     (params?: { status?: string; area?: string; severity?: string }) =>
      apiClient.get<DashboardAlert[]>('/dashboard/alerts', params as any),
    qualityStrategic:  (p?: DashboardParams) => apiClient.get('/dashboard/quality/strategic', p as any),
    qualityTactical:   (p?: DashboardParams) => apiClient.get('/dashboard/quality/tactical', p as any),
    qualityOperational:(p?: DashboardParams) => apiClient.get('/dashboard/quality/operational', p as any),
    qualityLosses:     (p?: DashboardParams) => apiClient.get('/dashboard/quality/losses', p as any),
    logisticsStrategic:(p?: DashboardParams) => apiClient.get('/dashboard/logistics/strategic', p as any),
    logisticsTactical: (p?: DashboardParams) => apiClient.get('/dashboard/logistics/tactical', p as any),
    logisticsOps:      (p?: DashboardParams) => apiClient.get('/dashboard/logistics/operational', p as any),
    productionStrategy:(p?: DashboardParams) => apiClient.get('/dashboard/production/strategic', p as any),
    productionTactical:(p?: DashboardParams) => apiClient.get('/dashboard/production/tactical', p as any),
    productionOps:     (p?: DashboardParams) => apiClient.get('/dashboard/production/operational', p as any),
    commercialStrategy:(p?: DashboardParams) => apiClient.get('/dashboard/commercial/strategic', p as any),
    financialStrategy: (p?: DashboardParams) => apiClient.get('/dashboard/financial/strategic', p as any),
    financialOps:      (p?: DashboardParams) => apiClient.get('/dashboard/financial/operational', p as any),
    intelligence:      (p?: DashboardParams) => apiClient.get('/dashboard/intelligence/supplier-impact', p as any),
  }
}

// ─── HOOKS ────────────────────────────────────────────────────────────────────

/**
 * hooks/usePermission.ts
 */
export type UserRole = 
  'admin' | 'director' | 'manager' | 'quality_analyst' | 'logistics_analyst' |
  'production_supervisor' | 'financial_analyst' | 'commercial_analyst' |
  'operator' | 'external_consultant' | 'viewer'

const ROLE_RANK: Record<UserRole, number> = {
  viewer: 0, external_consultant: 1, operator: 2,
  production_supervisor: 3, logistics_analyst: 3,
  quality_analyst: 3, commercial_analyst: 3, financial_analyst: 3,
  manager: 8, director: 9, admin: 10,
}

export function usePermission(userRole: UserRole | undefined) {
  const canDo = (minRole: UserRole): boolean => {
    if (!userRole) return false
    return ROLE_RANK[userRole] >= ROLE_RANK[minRole]
  }

  const isVisible = (section: string): boolean => {
    const sectionMin: Record<string, UserRole> = {
      commercial_bi:  'commercial_analyst',
      financial_bi:   'financial_analyst',
      reconciliation: 'manager',
      intelligence:   'manager',
      admin:          'admin',
      audit_log:      'director',
      cost_parameters:'financial_analyst',
    }
    const min = sectionMin[section]
    return min ? canDo(min) : true
  }

  return { canDo, isVisible }
}

/**
 * hooks/useImportPoller.ts
 * Polls import file status until it reaches a terminal state.
 */
import { useState, useEffect, useRef } from 'react'

const TERMINAL_STATUSES = new Set([
  'validated', 'validated_with_errors', 'imported',
  'imported_with_errors', 'failed', 'cancelled',
])

export function useImportPoller(fileId: string | null, intervalMs = 2000) {
  const [status, setStatus] = useState<string | null>(null)
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    if (!fileId) return
    const poll = async () => {
      const resp = await api.getImportFile(fileId)
      if (!resp.success || !resp.data) {
        setError(resp.error?.message ?? 'Erro ao verificar status.')
        clearInterval(intervalRef.current!)
        return
      }
      setData(resp.data)
      setStatus(resp.data.status)
      if (TERMINAL_STATUSES.has(resp.data.status)) {
        clearInterval(intervalRef.current!)
      }
    }
    poll() // immediate first call
    intervalRef.current = setInterval(poll, intervalMs)
    return () => clearInterval(intervalRef.current!)
  }, [fileId, intervalMs])

  return { status, data, error, isPolling: !!fileId && !TERMINAL_STATUSES.has(status ?? '') }
}

/**
 * hooks/useFilters.ts
 * URL-synced dashboard filter state.
 * Reads from searchParams; writes update the URL without full navigation.
 */
export type DashboardFilters = {
  date_from?: string
  date_to?: string
  supplier_id?: string
  product_id?: string
  driver_id?: string
  sector?: string
  is_reconciled?: boolean
}

// Placeholder type declarations to keep TypeScript happy
type UserProfile = any
type Lot = any; type LotDetail = any; type CreateLotBody = any; type LotListParams = any
type LotMovement = any; type CreateMovementBody = any
type Inspection = any; type InspectionDetail = any; type CreateInspectionBody = any
type InspectionListParams = any; type DefectItem = any; type CreateDefectItemBody = any
type NC = any; type NCListParams = any; type CreateNCBody = any; type UpdateNCBody = any
type ActionPlan = any; type CreateActionPlanBody = any
type ImportFile = any; type ImportSummary = any; type StagingRow = any; type ImportLog = any
type ReconciliationRun = any; type ReconciliationItem = any; type ReconciliationException = any
type CreateRunBody = any; type ResolveItemBody = any
type ExecutiveOverview = any; type DashboardAlert = any; type DashboardParams = any
