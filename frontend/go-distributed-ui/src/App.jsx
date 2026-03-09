import { useState, useRef, useEffect, useCallback } from 'react'
import './App.css'
import { db } from './firebase.js'
import { collection, addDoc, serverTimestamp } from 'firebase/firestore'
import {
  Upload,
  SlidersHorizontal,
  ArrowRight,
  Github,
  Linkedin,
  Mail,
  AlertCircle,
  ExternalLink,
  Send,
  CheckCircle2,
  ChevronDown,
  FileText,
  X,
  Loader2,
  Download,
  ChevronRight,
  Database,
  Eye,
  FileDown,
  MessageSquare,
  Table2,
  Link,
  Archive,
  Split,
  Layers,
} from 'lucide-react'

// ──────────────────────────────────────────────────
// Atoms
// ──────────────────────────────────────────────────

function StatusDot() {
  return (
    <span className="relative inline-flex h-2 w-2">
      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-done opacity-60" />
      <span className="relative inline-flex h-2 w-2 rounded-full bg-done" />
    </span>
  )
}

function Pill({ children }) {
  return (
    <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full border border-border bg-surface text-xs font-medium text-ink-muted font-mono">
      {children}
    </span>
  )
}

function StageChip({ index, label, active }) {
  return (
    <div
      className={`flex items-center gap-2 px-3 py-2 rounded-lg border text-xs font-mono transition-colors ${
        active
          ? 'bg-accent-light border-accent text-accent-text font-semibold'
          : 'bg-surface border-border text-ink-faint'
      }`}
    >
      <span
        className={`flex items-center justify-center w-4 h-4 rounded-full text-[10px] font-bold ${
          active ? 'bg-accent text-white' : 'bg-surface-raised text-ink-faint'
        }`}
      >
        {index}
      </span>
      {label}
    </div>
  )
}

// ──────────────────────────────────────────────────
// Contact Form
// ──────────────────────────────────────────────────

const REPORT_TYPES = [
  { value: 'bug',        label: 'Bug report' },
  { value: 'suggestion', label: 'Suggestion' },
  { value: 'question',   label: 'Question' },
  { value: 'other',      label: 'Other' },
]

function ContactForm() {
  const [form, setForm] = useState({ type: '', title: '', description: '', email: '' })
  const [sent, setSent] = useState(false)
  const [errors, setErrors] = useState({})

  const validate = () => {
    const e = {}
    if (!form.type)        e.type        = 'Please select a report type.'
    if (!form.title.trim()) e.title       = 'Title is required.'
    if (form.title.trim().length < 8)      e.title = 'Title must be at least 8 characters.'
    if (!form.description.trim())          e.description = 'Description is required.'
    if (form.description.trim().length < 20) e.description = 'Please provide at least 20 characters.'
    if (form.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(form.email))
      e.email = 'Enter a valid email address.'
    return e
  }

  const [submitting, setSubmitting] = useState(false)
  const [storeError, setStoreError] = useState(false)

  const buildGitHubUrl = () => {
    const repo = 'https://github.com/madhavbhayani/DataForge-ETL-Engine-Go-Distributed-Task-Processing/issues/new'
    const label = encodeURIComponent(form.type)
    const title = encodeURIComponent(form.title)
    const body  = encodeURIComponent(
      `**Type:** ${REPORT_TYPES.find(t => t.value === form.type)?.label ?? form.type}\n\n` +
      `**Description:**\n${form.description}` +
      (form.email ? `\n\n**Contact:** ${form.email}` : '')
    )
    return `${repo}?labels=${label}&title=${title}&body=${body}`
  }

  const handleSubmit = async (ev) => {
    ev.preventDefault()
    const e = validate()
    if (Object.keys(e).length) { setErrors(e); return }
    setErrors({})
    setSubmitting(true)
    setStoreError(false)

    const ghUrl = buildGitHubUrl()

    try {
      await addDoc(collection(db, 'issues_raised'), {
        type:        form.type,
        title:       form.title,
        description: form.description,
        email:       form.email || null,
        created_at:  serverTimestamp(),
        user_agent:  navigator.userAgent,
      })
    } catch {
      setStoreError(true)
    }

    window.open(ghUrl, '_blank', 'noopener')
    setSent(true)
    setForm({ type: '', title: '', description: '', email: '' })
    setSubmitting(false)
    setTimeout(() => { setSent(false); setStoreError(false) }, 5000)
  }

  const field = (key) => ({
    value: form[key],
    onChange: (ev) => {
      setForm(f => ({ ...f, [key]: ev.target.value }))
      if (errors[key]) setErrors(e => ({ ...e, [key]: undefined }))
    },
  })

  return (
    <section id="feedback" className="bg-canvas border-t border-border">
      <div className="mx-auto max-w-5xl px-6 py-20">

        {/* Header */}
        <div className="mb-10">
          <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full border border-border bg-surface text-xs font-mono text-ink-muted mb-4">
            <AlertCircle size={11} />
            Feedback
          </span>
          <h2
            style={{ fontFamily: 'var(--font-display)' }}
            className="text-3xl font-bold text-ink leading-tight mb-2"
          >
            Found a bug? Have a suggestion?
          </h2>
          <p className="text-sm text-ink-faint max-w-lg leading-relaxed">
            Report issues or propose improvements. Your submission opens a
            pre-filled GitHub issue — no account needed to draft it.
          </p>
        </div>

        <form onSubmit={handleSubmit} noValidate className="grid grid-cols-1 md:grid-cols-2 gap-5">

          {/* Type select */}
          <div className="flex flex-col gap-1.5 md:col-span-2 md:w-64">
            <label className="text-xs font-semibold text-ink-muted uppercase tracking-wider">
              Report type <span className="text-failed">*</span>
            </label>
            <div className="relative">
              <select
                {...field('type')}
                className={`w-full appearance-none bg-surface border rounded-lg px-3.5 py-2.5 text-sm text-ink
                  focus:outline-none focus:ring-2 focus:ring-accent/30 focus:border-accent transition-colors cursor-pointer
                  ${errors.type ? 'border-failed' : 'border-border hover:border-border-strong'}`}
              >
                <option value="" disabled>Select type…</option>
                {REPORT_TYPES.map(t => (
                  <option key={t.value} value={t.value}>{t.label}</option>
                ))}
              </select>
              <ChevronDown size={14} className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-ink-faint" />
            </div>
            {errors.type && <p className="text-xs text-failed">{errors.type}</p>}
          </div>

          {/* Title */}
          <div className="flex flex-col gap-1.5 md:col-span-2">
            <label className="text-xs font-semibold text-ink-muted uppercase tracking-wider">
              Title <span className="text-failed">*</span>
            </label>
            <input
              type="text"
              placeholder="Short, descriptive title of the issue"
              {...field('title')}
              className={`bg-surface border rounded-lg px-3.5 py-2.5 text-sm text-ink placeholder:text-ink-faint
                focus:outline-none focus:ring-2 focus:ring-accent/30 focus:border-accent transition-colors
                ${errors.title ? 'border-failed' : 'border-border hover:border-border-strong'}`}
            />
            {errors.title && <p className="text-xs text-failed">{errors.title}</p>}
          </div>

          {/* Description */}
          <div className="flex flex-col gap-1.5 md:col-span-2">
            <label className="text-xs font-semibold text-ink-muted uppercase tracking-wider">
              Description <span className="text-failed">*</span>
            </label>
            <textarea
              rows={5}
              placeholder="Describe the issue or suggestion in detail. Include steps to reproduce if it's a bug."
              {...field('description')}
              className={`bg-surface border rounded-lg px-3.5 py-2.5 text-sm text-ink placeholder:text-ink-faint resize-y
                focus:outline-none focus:ring-2 focus:ring-accent/30 focus:border-accent transition-colors
                ${errors.description ? 'border-failed' : 'border-border hover:border-border-strong'}`}
            />
            {errors.description && <p className="text-xs text-failed">{errors.description}</p>}
          </div>

          {/* Email (optional) */}
          <div className="flex flex-col gap-1.5">
            <label className="text-xs font-semibold text-ink-muted uppercase tracking-wider">
              Email <span className="text-ink-faint font-normal normal-case tracking-normal">(optional)</span>
            </label>
            <input
              type="email"
              placeholder="your@email.com"
              {...field('email')}
              className={`bg-surface border rounded-lg px-3.5 py-2.5 text-sm text-ink placeholder:text-ink-faint
                focus:outline-none focus:ring-2 focus:ring-accent/30 focus:border-accent transition-colors
                ${errors.email ? 'border-failed' : 'border-border hover:border-border-strong'}`}
            />
            {errors.email && <p className="text-xs text-failed">{errors.email}</p>}
          </div>

          {/* Submit */}
          <div className="flex items-end">
            <button
              type="submit"
              disabled={submitting}
              className="inline-flex items-center gap-2 px-6 py-2.5 rounded-lg bg-accent hover:bg-accent-hover disabled:opacity-60
                text-white text-sm font-semibold transition-colors duration-150 cursor-pointer shadow-sm"
            >
              {submitting ? <Loader2 size={14} strokeWidth={2.5} className="animate-spin" /> : <Send size={14} strokeWidth={2.5} />}
              {submitting ? 'Submitting…' : 'Open on GitHub'}
            </button>
          </div>

        </form>

        {/* Success toast */}
        {sent && (
          <div className="mt-5 inline-flex items-center gap-2 px-4 py-2.5 rounded-lg bg-done-bg border border-done text-done text-sm font-medium">
            <CheckCircle2 size={15} strokeWidth={2} />
            {storeError ? 'Redirecting to GitHub Issues…' : 'Issue saved & opened on GitHub.'}
          </div>
        )}

      </div>
    </section>
  )
}

// ──────────────────────────────────────────────────
// About / Footer
// ──────────────────────────────────────────────────

function AboutSection() {
  return (
    <section className="bg-surface-raised border-t border-border">
      <div className="mx-auto max-w-5xl px-6 py-16">

        <div className="grid grid-cols-1 md:grid-cols-3 gap-10">

          {/* Project info */}
          <div className="md:col-span-2 flex flex-col gap-4">
            <h3
              style={{ fontFamily: 'var(--font-display)' }}
              className="text-xl font-bold text-ink"
            >
              About This Project
            </h3>
            <p className="text-sm text-ink-muted leading-relaxed max-w-lg">
              DataForge is a <span className="text-ink font-medium">learning project</span> by{' '}
              <span className="text-ink font-medium">Madhav Bhayani</span>, created to
              dive deep into distributed systems, Go concurrency patterns, and
              real-world ETL pipeline design. Every component — from the
              concurrent worker pool and priority job queue to the intelligent
              CSV column analyzer and multi-stage transformation engine — is
              hand-built from scratch without relying on external frameworks.
              The goal: learn by building something non-trivial, end to end.
            </p>
            <div className="flex flex-wrap gap-2 mt-1">
              {['Go 1.25', 'chi router', 'React 19', 'Tailwind v4', 'MIT License'].map(tag => (
                <span key={tag} className="px-2.5 py-1 rounded-md bg-surface border border-border text-xs font-mono text-ink-faint">
                  {tag}
                </span>
              ))}
            </div>

            {/* Raise issue CTA */}
            <div className="mt-2 p-4 rounded-xl border border-border bg-surface flex flex-col sm:flex-row items-start sm:items-center gap-3">
              <div className="flex-1">
                <p className="text-sm font-semibold text-ink">View the repository on GitHub</p>
                <p className="text-xs text-ink-faint mt-0.5">
                  Star the project, browse source, or raise an issue directly.
                </p>
              </div>
              <div className="flex gap-2 flex-shrink-0">
                <a
                  href="https://github.com/madhavbhayani/DataForge-ETL-Engine-Go-Distributed-Task-Processing"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg bg-surface-raised hover:bg-surface-high
                    text-ink text-xs font-semibold border border-border hover:border-border-strong transition-colors"
                >
                  <Github size={13} strokeWidth={2} />
                  View repo
                </a>
                
              </div>
            </div>

            {/* Product Hunt badge */}
            <div className="mt-2">
              <a
                href="https://www.producthunt.com/products/dataforge-2?embed=true&utm_source=badge-featured&utm_medium=badge&utm_campaign=badge-dataforge-2"
                target="_blank"
                rel="noopener noreferrer"
              >
                <img
                  src="https://api.producthunt.com/widgets/embed-image/v1/featured.svg?post_id=1093417&theme=light&t=1773054396066"
                  alt="DataForge - Distributed ETL pipeline engine. | Product Hunt"
                  width="250"
                  height="54"
                />
              </a>
            </div>
          </div>

          {/* Contact links */}
          <div className="flex flex-col gap-4">
            <h3
              style={{ fontFamily: 'var(--font-display)' }}
              className="text-xl font-bold text-ink"
            >
              Get in touch
            </h3>
            <p className="text-xs text-ink-faint leading-relaxed">
              Personal learning project by Madhav Bhayani.<br />
              Open to feedback, questions, and collaboration.
            </p>

            <div className="flex flex-col gap-2">
              <a
                href="https://github.com/madhavbhayani/go-distributed-task-engine"
                target="_blank"
                rel="noopener noreferrer"
                className="group flex items-center gap-3 px-4 py-3 rounded-lg bg-surface border border-border
                  hover:border-border-strong transition-colors"
              >
                <span className="flex items-center justify-center w-8 h-8 rounded-lg bg-surface-raised border border-border group-hover:border-border-strong transition-colors">
                  <Github size={15} className="text-ink-muted group-hover:text-ink transition-colors" strokeWidth={2} />
                </span>
                <div className="flex flex-col min-w-0">
                  <span className="text-xs font-semibold text-ink">GitHub</span>
                  <span className="text-[11px] text-ink-faint font-mono truncate">madhavbhayani</span>
                </div>
                <ExternalLink size={12} className="ml-auto text-ink-faint opacity-0 group-hover:opacity-100 transition-opacity" />
              </a>

              <a
                href="https://linkedin.com/in/madhavbhayani"
                target="_blank"
                rel="noopener noreferrer"
                className="group flex items-center gap-3 px-4 py-3 rounded-lg bg-surface border border-border
                  hover:border-border-strong transition-colors"
              >
                <span className="flex items-center justify-center w-8 h-8 rounded-lg bg-surface-raised border border-border group-hover:border-border-strong transition-colors">
                  <Linkedin size={15} className="text-ink-muted group-hover:text-ink transition-colors" strokeWidth={2} />
                </span>
                <div className="flex flex-col min-w-0">
                  <span className="text-xs font-semibold text-ink">LinkedIn</span>
                  <span className="text-[11px] text-ink-faint font-mono truncate">madhavbhayani</span>
                </div>
                <ExternalLink size={12} className="ml-auto text-ink-faint opacity-0 group-hover:opacity-100 transition-opacity" />
              </a>

              <a
                href="mailto:madhavbhayani21@gmail.com"
                className="group flex items-center gap-3 px-4 py-3 rounded-lg bg-surface border border-border
                  hover:border-border-strong transition-colors"
              >
                <span className="flex items-center justify-center w-8 h-8 rounded-lg bg-surface-raised border border-border group-hover:border-border-strong transition-colors">
                  <Mail size={15} className="text-ink-muted group-hover:text-ink transition-colors" strokeWidth={2} />
                </span>
                <div className="flex flex-col min-w-0">
                  <span className="text-xs font-semibold text-ink">Email</span>
                  <span className="text-[11px] text-ink-faint font-mono truncate">madhavbhayani21@gmail.com</span>
                </div>
              </a>
            </div>
          </div>

        </div>

        {/* Footer bar */}
        <div className="mt-12 pt-6 border-t border-border flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3">
          <p className="text-xs text-ink-faint font-mono">
            DataForge · Personal project by Madhav Bhayani · MIT License
          </p>
          <div className="flex items-center gap-4">
            <a
              href="https://github.com/madhavbhayani/DataForge-ETL-Engine-Go-Distributed-Task-Processing"
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-ink-faint hover:text-ink transition-colors font-mono"
            >
              Source code
            </a>
            <span className="text-border-strong">·</span>
            <a
              href="https://github.com/madhavbhayani/DataForge-ETL-Engine-Go-Distributed-Task-Processing/issues"
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-ink-faint hover:text-ink transition-colors font-mono"
            >
              Issue tracker
            </a>
          </div>
        </div>

      </div>
    </section>
  )
}



// ──────────────────────────────────────────────────
// CSV File Picker hook (validate only — upload in App)
// ──────────────────────────────────────────────────

const MAX_CSV_BYTES = 10 * 1024 * 1024 // 10 MB

function useCSVPicker() {
  const inputRef = useRef(null)
  const [file, setFile]   = useState(null)
  const [error, setError] = useState('')

  const open = () => inputRef.current?.click()

  const onChange = (ev) => {
    const picked = ev.target.files?.[0]
    ev.target.value = ''
    if (!picked) return
    if (!picked.name.toLowerCase().endsWith('.csv')) {
      setError('Only .csv files are supported.')
      setFile(null)
      return
    }
    if (picked.size > MAX_CSV_BYTES) {
      setError(`File exceeds 10 MB limit (${(picked.size / 1024 / 1024).toFixed(1)} MB).`)
      setFile(null)
      return
    }
    setError('')
    setFile(picked)
  }

  const clear = () => { setFile(null); setError('') }

  return { inputRef, file, error, open, onChange, clear }
}

// ──────────────────────────────────────────────────
// Backend API utilities
// ──────────────────────────────────────────────────

const BACKEND = import.meta.env.VITE_API_BASE_URL

const sleep = (ms) => new Promise(r => setTimeout(r, ms))

async function pollJob(jobId, signal) {
  for (let i = 0; i < 120; i++) {
    await sleep(1000)
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
    const res  = await fetch(`${BACKEND}/api/v1/jobs/${jobId}`, { signal })
    const data = await res.json()
    const job  = data.data
    if (['completed', 'failed', 'cancelled'].includes(job.status)) return job
  }
  throw new Error('Job timed out after 2 minutes')
}

function buildCleanRules(analysis, columns, opts = {}) {
  const rules = []

  // If drop empty columns is enabled, add it as the first rule
  if (opts.dropEmptyColumns) {
    rules.push({ column: '', operation: 'drop_empty_columns', params: { min_fill_rate: String(opts.minFillRate ?? 0) } })
  }

  // Global null handling strategy from ETL options
  const nullStrategy = opts.nullHandling ?? 'fill_default'

  for (const col of columns) {
    const colAnalysis = analysis?.columns?.[col]
    for (const rec of colAnalysis?.recommended_clean_ops ?? []) {
      const rule = { column: rec.column ?? col, operation: rec.operation, params: { ...(rec.params ?? {}) } }

      // When "skip" is selected, remove fill_null rules entirely
      if (rule.operation === 'fill_null' && nullStrategy === 'skip') {
        continue // skip this rule — don't add it
      }

      // Override null-handling rules based on user's global strategy
      if (rule.operation === 'fill_null' && nullStrategy !== 'skip') {
        if (nullStrategy === 'drop') {
          rule.operation = 'drop_null'
          rule.params = {}
        } else if (nullStrategy === 'fill_mean') {
          rule.params.strategy = 'mean'
        } else if (nullStrategy === 'fill_median') {
          rule.params.strategy = 'median'
        } else if (nullStrategy === 'fill_custom') {
          const customVal = opts.customFillValues?.[rule.column]
          if (customVal != null && customVal !== '') {
            rule.params = { fill_value: customVal }
          } else {
            rule.params.strategy = 'mode'
          }
        }
        // fill_default keeps the analyzer's recommendation (uses mode on backend)
      }

      // If user selects "drop", convert fill_null → drop_null
      if (rec.operation === 'drop_null' && (nullStrategy === 'fill_default' || nullStrategy === 'fill_custom')) {
        rule.operation = 'fill_null'
        rule.params = { fill_value: colAnalysis?.mode_value || 'Unknown', strategy: 'mode' }
      }

      rules.push(rule)
    }
  }

  // Validator requires at least 1 rule — always trim whitespace as a safe baseline
  if (rules.length === 0) rules.push({ column: '', operation: 'trim_whitespace' })
  return rules
}

function checkNormalizationApplicable(analysis, columns) {
  // Database normalization is applicable when the dataset has:
  //   1. Categorical columns (low cardinality — good for lookup tables)
  //   2. A potential primary key (high uniqueness — ID column)
  const categoricalCols = []
  const potentialPKs = []
  const totalRows = analysis?.total_rows ?? 0

  for (const col of columns) {
    const ca = analysis?.columns?.[col]
    if (!ca) continue
    if (ca.inferred_type === 'categorical') {
      categoricalCols.push(col)
    }
    // A column with very high uniqueness (>90%) is a potential PK
    if (ca.unique_count > 0 && totalRows > 0) {
      const uniqueRatio = ca.unique_count / totalRows
      if (uniqueRatio >= 0.90) {
        potentialPKs.push(col)
      }
    }
  }

  return {
    applicable: categoricalCols.length > 0 || potentialPKs.length > 0,
    categoricalCols,
    potentialPKs,
  }
}

// ──────────────────────────────────────────────────
// Build value-level normalize rules from analysis
// ──────────────────────────────────────────────────

function buildNormalizeRules(_analysis, _columns) {
  // Value-level transforms (z_score, min_max_scale, etc.) are disabled
  // because they destructively replace original CSV values.
  // Only DB normalization (1NF/2NF/3NF decomposition) is used.
  return []
}

// ──────────────────────────────────────────────────
// Default ETL options
// ──────────────────────────────────────────────────

const DEFAULT_ETL_OPTIONS = {
  // Clean options
  nullHandling: 'fill_default',     // fill_default | skip | drop | fill_mean | fill_median | fill_custom
  customFillValues: {},              // { column_name: 'value', ... } — only used with fill_custom
  dropEmptyColumns: true,          // remove columns with 0% fill
  minFillRate: 0,                  // min fill rate for drop_empty_columns (0 = only fully empty)

  // Dedup options
  dedupStrategy: 'exact',          // exact | fuzzy
  fuzzyThreshold: 0.85,            // 0.0 – 1.0
  keepStrategy: 'first',           // first | last | most_complete
  dryRun: false,                   // preview only
  matchColumns: null,              // null = all columns
}

// ──────────────────────────────────────────────────
// useETLOptions hook
// ──────────────────────────────────────────────────

function useETLOptions() {
  const [opts, setOpts] = useState({ ...DEFAULT_ETL_OPTIONS })
  const patch = useCallback((updates) =>
    setOpts(prev => ({ ...prev, ...updates })), [])
  const reset = useCallback(() => setOpts({ ...DEFAULT_ETL_OPTIONS }), [])
  return { opts, patch, reset }
}

// ──────────────────────────────────────────────────
// usePipeline hook — step-by-step, manually triggered
// ──────────────────────────────────────────────────

function initStep() {
  return { status: 'idle', result: null, submitTime: null, endTime: null, jobId: null, error: null, cleanRules: null }
}

function usePipeline(etlOpts) {
  const [steps, setSteps] = useState({
    import:    initStep(),
    clean:     initStep(),
    normalize: initStep(),
    dedup:     initStep(),
  })
  const [viewStep, setViewStep]         = useState(0)
  const [normHasRules, setNormHasRules] = useState(false)
  const normRulesRef                    = useRef([])
  const [normApplicable, setNormApplicable] = useState(false)
  const [normInfo, setNormInfo]         = useState(null) // { categoricalCols, potentialPKs }
  const curDatasetIdRef                 = useRef(null)
  const abortRef                        = useRef(null)

  const patchStep = useCallback((id, patch) =>
    setSteps(prev => ({ ...prev, [id]: { ...prev[id], ...patch } })), [])

  const reset = useCallback(() => {
    abortRef.current?.abort()
    curDatasetIdRef.current = null
    setNormHasRules(false)
    normRulesRef.current = []
    setNormApplicable(false)
    setNormInfo(null)
    setSteps({ import: initStep(), clean: initStep(), normalize: initStep(), dedup: initStep() })
    setViewStep(0)
  }, [])

  const initImport = useCallback((uploadResult) => {
    curDatasetIdRef.current = uploadResult.dataset_id
    // Check database normalization applicability (value-level transforms are disabled)
    normRulesRef.current = []
    const info = checkNormalizationApplicable(uploadResult.analysis, uploadResult.columns)
    setNormApplicable(info.applicable)
    setNormInfo(info)
    setNormHasRules(info.applicable)
    patchStep('import', { status: 'done', result: uploadResult, endTime: Date.now() })
    setViewStep(0)
  }, [patchStep])

  const previewNormalize = useCallback(() => {
    const normRules = normRulesRef.current
    if (normRules.length === 0 && !normApplicable) {
      patchStep('normalize', { status: 'skipped' })
      setViewStep(3)
      return
    }
    patchStep('normalize', { status: 'preview', normRules, error: null })
    setViewStep(2)
  }, [patchStep, normApplicable])

  const previewClean = useCallback((uploadResult) => {
    const cleanRules = buildCleanRules(uploadResult.analysis, uploadResult.columns, etlOpts)
    patchStep('clean', { status: 'preview', cleanRules, error: null })
    setViewStep(1)
  }, [patchStep, etlOpts, setViewStep])

  const runClean = useCallback(async (uploadResult, prebuiltRules) => {
    abortRef.current?.abort()
    const ctrl = new AbortController()
    abortRef.current = ctrl
    const submitTime = Date.now()
    patchStep('clean', { status: 'running', submitTime, error: null })
    setViewStep(1)
    try {
      const cleanRules = prebuiltRules ?? buildCleanRules(uploadResult.analysis, uploadResult.columns, etlOpts)
      const cleanOutId = curDatasetIdRef.current + '_cleaned'
      const res = await fetch(`${BACKEND}/api/v1/jobs/etl/clean`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: `Clean: ${uploadResult.filename}`,
          params: {
            dataset_id:        curDatasetIdRef.current,
            rules:             cleanRules,
            null_handling:     etlOpts.nullHandling ?? 'fill_default',
            custom_fill_values: etlOpts.nullHandling === 'fill_custom' ? (etlOpts.customFillValues ?? {}) : undefined,
            create_copy:       true,
            output_dataset_id: cleanOutId,
          },
        }),
        signal: ctrl.signal,
      })
      const data = await res.json()
      if (!data.success) throw new Error(data.error?.message ?? 'Failed to submit clean job')
      const jobId = data.data.job_id
      patchStep('clean', { jobId })
      const job = await pollJob(jobId, ctrl.signal)
      if (job.status !== 'completed') throw new Error(`Clean job ${job.status}${job.error ? ': ' + job.error : ''}`)
      curDatasetIdRef.current = job.result?.dataset_id ?? cleanOutId
      patchStep('clean', { status: 'done', result: job.result, endTime: Date.now() })
    } catch (err) {
      if (err.name === 'AbortError') return
      patchStep('clean', { status: 'error', error: err.message })
    }
  }, [patchStep, etlOpts])

  const runNormalize = useCallback(async (uploadResult, prebuiltRules, dbConfig) => {
    const normRules = prebuiltRules ?? normRulesRef.current
    const hasRules = normRules.length > 0
    const hasDbNorm = (dbConfig?.normalForm ?? 0) > 0
    if (!hasRules && !hasDbNorm) {
      patchStep('normalize', { status: 'skipped' })
      setViewStep(3)
      return
    }
    abortRef.current?.abort()
    const ctrl = new AbortController()
    abortRef.current = ctrl
    const submitTime = Date.now()
    patchStep('normalize', { status: 'running', submitTime, error: null })
    setViewStep(2)
    try {
      const normOutId = curDatasetIdRef.current + '_normalized'
      const params = {
        dataset_id:        curDatasetIdRef.current,
        create_copy:       true,
        output_dataset_id: normOutId,
      }
      if (hasRules) params.rules = normRules
      if (hasDbNorm) {
        params.normal_form        = dbConfig.normalForm
        params.primary_key_column = dbConfig.primaryKeyColumn ?? ''
        params.categorical_columns = dbConfig.categoricalColumns ?? []
      }
      const res = await fetch(`${BACKEND}/api/v1/jobs/etl/normalize`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: `Normalize: ${uploadResult.filename}`,
          params,
        }),
        signal: ctrl.signal,
      })
      const data = await res.json()
      if (!data.success) throw new Error(data.error?.message ?? 'Failed to submit normalize job')
      const jobId = data.data.job_id
      patchStep('normalize', { jobId })
      const job = await pollJob(jobId, ctrl.signal)
      if (job.status !== 'completed') throw new Error(`Normalize job ${job.status}${job.error ? ': ' + job.error : ''}`)
      curDatasetIdRef.current = job.result?.dataset_id ?? normOutId
      patchStep('normalize', { status: 'done', result: job.result, endTime: Date.now() })
    } catch (err) {
      if (err.name === 'AbortError') return
      patchStep('normalize', { status: 'error', error: err.message })
    }
  }, [patchStep])

  const runDedup = useCallback(async (uploadResult) => {
    abortRef.current?.abort()
    const ctrl = new AbortController()
    abortRef.current = ctrl
    const submitTime = Date.now()
    patchStep('dedup', { status: 'running', submitTime, error: null })
    setViewStep(3)
    try {
      const dedupOutId = curDatasetIdRef.current + '_deduped'
      const matchCols = etlOpts.matchColumns?.length ? etlOpts.matchColumns : uploadResult.columns
      const res = await fetch(`${BACKEND}/api/v1/jobs/etl/deduplicate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: `Dedup: ${uploadResult.filename}`,
          params: {
            dataset_id:        curDatasetIdRef.current,
            match_columns:     matchCols,
            strategy:          etlOpts.dedupStrategy ?? 'exact',
            fuzzy_threshold:   etlOpts.dedupStrategy === 'fuzzy' ? (etlOpts.fuzzyThreshold ?? 0.85) : undefined,
            keep_strategy:     etlOpts.keepStrategy ?? 'first',
            dry_run:           etlOpts.dryRun ?? false,
            create_copy:       true,
            output_dataset_id: dedupOutId,
          },
        }),
        signal: ctrl.signal,
      })
      const data = await res.json()
      if (!data.success) throw new Error(data.error?.message ?? 'Failed to submit dedup job')
      const jobId = data.data.job_id
      patchStep('dedup', { jobId })
      const job = await pollJob(jobId, ctrl.signal)
      if (job.status !== 'completed') throw new Error(`Dedup job ${job.status}${job.error ? ': ' + job.error : ''}`)
      patchStep('dedup', { status: 'done', result: job.result, params: job.params, endTime: Date.now() })
    } catch (err) {
      if (err.name === 'AbortError') return
      patchStep('dedup', { status: 'error', error: err.message })
    }
  }, [patchStep, etlOpts])

  return { steps, viewStep, setViewStep, normHasRules, normApplicable, normInfo, initImport, previewClean, runClean, previewNormalize, runNormalize, runDedup, reset }
}

// ──────────────────────────────────────────────────
// Import Preview Panel
// ──────────────────────────────────────────────────

const TYPE_COLOR = {
  integer:        'bg-blue-50 text-blue-700 border-blue-200',
  float:          'bg-blue-50 text-blue-700 border-blue-200',
  boolean:        'bg-violet-50 text-violet-700 border-violet-200',
  date:           'bg-teal-50 text-teal-700 border-teal-200',
  datetime:       'bg-teal-50 text-teal-700 border-teal-200',
  email:          'bg-orange-50 text-orange-700 border-orange-200',
  phone:          'bg-orange-50 text-orange-700 border-orange-200',
  url:            'bg-pink-50 text-pink-700 border-pink-200',
  categorical:    'bg-rose-50 text-rose-700 border-rose-200',
  alphanumeric:   'bg-slate-100 text-slate-600 border-slate-200',
  numeric_string: 'bg-indigo-50 text-indigo-700 border-indigo-200',
  free_text:      'bg-slate-100 text-slate-500 border-slate-200',
}

function QualityBar({ value }) {
  const pct   = Math.round((value ?? 0) * 100)
  const color = pct >= 80 ? 'bg-done' : pct >= 50 ? 'bg-yellow-400' : 'bg-failed'
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 rounded-full bg-surface-raised overflow-hidden">
        <div className={`h-full rounded-full transition-all duration-700 ${color}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-[11px] font-mono text-ink-muted w-7 text-right">{pct}%</span>
    </div>
  )
}

// ──────────────────────────────────────────────────
// Quality Delta — shows before → after quality change
// ──────────────────────────────────────────────────

function QualityDelta({ before, after }) {
  if (after == null) return null
  const beforePct = Math.round((before ?? 0) * 100)
  const afterPct  = Math.round(after * 100)
  const diff      = afterPct - beforePct
  const diffColor = diff > 0 ? 'text-done' : diff < 0 ? 'text-failed' : 'text-ink-faint'
  return (
    <div className="flex items-center gap-3">
      <div className="flex-1">
        <QualityBar value={after} />
      </div>
      {diff !== 0 && (
        <span className={`text-[11px] font-mono font-semibold ${diffColor}`}>
          {diff > 0 ? '+' : ''}{diff}%
        </span>
      )}
    </div>
  )
}

// ──────────────────────────────────────────────────
// ETL Options Panel — advanced controls
// ──────────────────────────────────────────────────

const NULL_HANDLING_OPTIONS = [
  { value: 'fill_default',  label: 'Fill default',  desc: 'Replace with most frequent value (mode) in the column' },
  { value: 'fill_mean',     label: 'Fill mean',     desc: 'Replace with column mean (numeric)' },
  { value: 'fill_median',   label: 'Fill median',   desc: 'Replace with column median (numeric)' },
  { value: 'fill_custom',   label: 'Fill custom',   desc: 'Fill with custom values per column (set in clean preview)' },
  { value: 'skip',          label: 'Skip',           desc: 'Leave null values as-is (no filling)' },
  { value: 'drop',          label: 'Drop rows',      desc: 'Remove entire rows that contain null values' },
]

const KEEP_STRATEGY_OPTIONS = [
  { value: 'first',         label: 'First',        desc: 'Keep the first occurrence' },
  { value: 'last',          label: 'Last',         desc: 'Keep the last occurrence' },
  { value: 'most_complete', label: 'Most complete', desc: 'Keep the row with fewest nulls' },
]

function ETLOptionsDialog({ open, onClose, opts, patch, columns, uploadResult }) {
  if (!open) return null

  // Find columns that have null/missing values (null_count = literal "null"/"N/A", empty_count = empty strings)
  const nullableCols = (columns ?? []).filter(col => {
    const ca = uploadResult?.analysis?.columns?.[col]
    const missing = (ca?.null_count ?? 0) + (ca?.empty_count ?? 0)
    return missing > 0
  })

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/40 backdrop-blur-sm" onClick={onClose} />

      {/* Dialog */}
      <div className="relative w-full max-w-lg mx-4 rounded-xl border border-border bg-surface shadow-2xl overflow-hidden animate-in fade-in zoom-in-95">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-border bg-surface-raised">
          <div className="flex items-center gap-2">
            <SlidersHorizontal size={14} className="text-accent" strokeWidth={2.5} />
            <span className="text-sm font-semibold text-ink">Advanced ETL Options</span>
          </div>
          <button onClick={onClose} className="text-ink-faint hover:text-ink transition-colors cursor-pointer p-0.5 rounded hover:bg-border">
            <X size={14} strokeWidth={2.5} />
          </button>
        </div>

        {/* Body */}
        <div className="px-5 py-4 space-y-5 max-h-[70vh] overflow-y-auto">
          {/* ── Clean Options ── */}
          <div>
            <p className="text-[11px] font-semibold text-ink-muted uppercase tracking-wider mb-2.5">Cleaning</p>
            <div className="space-y-3">
              {/* Null Handling */}
              <div>
                <label className="text-xs text-ink-muted font-medium">Null handling</label>
                <div className="flex flex-wrap gap-1.5 mt-1.5">
                  {NULL_HANDLING_OPTIONS.map(o => (
                    <button
                      key={o.value}
                      onClick={() => patch({ nullHandling: o.value })}
                      title={o.desc}
                      className={`px-2.5 py-1 rounded-md text-[11px] font-medium border transition-colors cursor-pointer ${
                        opts.nullHandling === o.value
                          ? 'bg-accent text-white border-accent'
                          : 'bg-surface-raised text-ink-muted border-border hover:border-ink-faint'
                      }`}
                    >
                      {o.label}
                    </button>
                  ))}
                </div>
              </div>

              {/* Custom fill values hint */}
              {opts.nullHandling === 'fill_custom' && nullableCols.length > 0 && (
                <div className="rounded-lg border border-blue-200 bg-blue-50/50 p-2.5">
                  <p className="text-[10px] font-semibold text-blue-700 mb-1.5">Custom Fill Values</p>
                  <p className="text-[9px] text-blue-600 mb-2">Enter custom values for each nullable column. Values must match the column's data type.</p>
                  <div className="space-y-1.5 max-h-[180px] overflow-y-auto">
                    {nullableCols.map(col => {
                      const ca = uploadResult?.analysis?.columns?.[col]
                      const dtype = ca?.inferred_type ?? 'text'
                      const missingCount = (ca?.null_count ?? 0) + (ca?.empty_count ?? 0)
                      const inputType = (dtype === 'integer' || dtype === 'float') ? 'number' : 'text'
                      return (
                        <div key={col} className="flex items-center gap-2 bg-surface rounded-lg border border-border px-2 py-1.5">
                          <div className="flex-1 min-w-0">
                            <span className="text-[10px] font-mono font-semibold text-ink truncate block">{col}</span>
                            <span className="text-[8px] text-ink-faint">{dtype} · {missingCount} missing</span>
                          </div>
                          <input
                            type={inputType}
                            value={opts.customFillValues?.[col] ?? ''}
                            onChange={e => patch({ customFillValues: { ...opts.customFillValues, [col]: e.target.value } })}
                            placeholder={dtype === 'integer' ? '0' : dtype === 'float' ? '0.0' : dtype === 'boolean' ? 'false' : 'value'}
                            className="w-24 text-[10px] font-mono rounded border border-border bg-surface-raised px-2 py-1 text-ink focus:border-accent focus:ring-1 focus:ring-accent outline-none"
                          />
                        </div>
                      )
                    })}
                  </div>
                </div>
              )}
              {opts.nullHandling === 'fill_custom' && nullableCols.length === 0 && (
                <p className="text-[10px] text-ink-faint italic">Upload a CSV first to see nullable columns and enter custom fill values.</p>
              )}

              {/* Drop Empty Columns */}
              <div className="flex items-center gap-2">
                <label className="flex items-center gap-2 text-xs text-ink-muted cursor-pointer">
                  <input
                    type="checkbox"
                    checked={opts.dropEmptyColumns}
                    onChange={e => patch({ dropEmptyColumns: e.target.checked })}
                    className="rounded border-border accent-accent w-3.5 h-3.5"
                  />
                  <span className="font-medium">Drop empty columns</span>
                </label>
                <span className="text-[10px] text-ink-faint">(removes columns with 0% fill)</span>
              </div>
            </div>
          </div>

          <div className="h-px bg-border" />

          {/* ── Dedup Options ── */}
          <div>
            <p className="text-[11px] font-semibold text-ink-muted uppercase tracking-wider mb-2.5">Deduplication</p>
            <div className="space-y-3">
              {/* Strategy */}
              <div>
                <label className="text-xs text-ink-muted font-medium">Strategy</label>
                <div className="flex gap-2 mt-1.5">
                  {['exact', 'fuzzy'].map(s => (
                    <button
                      key={s}
                      onClick={() => patch({ dedupStrategy: s })}
                      className={`px-3.5 py-1 rounded-md text-[11px] font-medium border transition-colors cursor-pointer ${
                        opts.dedupStrategy === s
                          ? 'bg-accent text-white border-accent'
                          : 'bg-surface-raised text-ink-muted border-border hover:border-ink-faint'
                      }`}
                    >
                      {s === 'exact' ? 'Exact match' : 'Fuzzy match'}
                    </button>
                  ))}
                </div>
              </div>

              {/* Fuzzy Threshold (only when fuzzy) */}
              {opts.dedupStrategy === 'fuzzy' && (
                <div>
                  <label className="text-xs text-ink-muted font-medium flex items-center justify-between">
                    <span>Similarity threshold</span>
                    <span className="font-mono text-accent font-semibold">{opts.fuzzyThreshold.toFixed(2)}</span>
                  </label>
                  <input
                    type="range"
                    min="0.1"
                    max="1"
                    step="0.05"
                    value={opts.fuzzyThreshold}
                    onChange={e => patch({ fuzzyThreshold: parseFloat(e.target.value) })}
                    className="w-full h-1.5 rounded-full appearance-none bg-border accent-accent mt-1.5 cursor-pointer"
                  />
                  <div className="flex justify-between text-[9px] text-ink-faint mt-1">
                    <span>Loose (0.1)</span>
                    <span>Strict (1.0)</span>
                  </div>
                </div>
              )}

              {/* Keep Strategy */}
              <div>
                <label className="text-xs text-ink-muted font-medium">Keep strategy</label>
                <div className="flex flex-wrap gap-1.5 mt-1.5">
                  {KEEP_STRATEGY_OPTIONS.map(o => (
                    <button
                      key={o.value}
                      onClick={() => patch({ keepStrategy: o.value })}
                      title={o.desc}
                      className={`px-2.5 py-1 rounded-md text-[11px] font-medium border transition-colors cursor-pointer ${
                        opts.keepStrategy === o.value
                          ? 'bg-accent text-white border-accent'
                          : 'bg-surface-raised text-ink-muted border-border hover:border-ink-faint'
                      }`}
                    >
                      {o.label}
                    </button>
                  ))}
                </div>
              </div>

              {/* Dry Run */}
              <label className="flex items-center gap-2 text-xs text-ink-muted cursor-pointer">
                <input
                  type="checkbox"
                  checked={opts.dryRun}
                  onChange={e => patch({ dryRun: e.target.checked })}
                  className="rounded border-border accent-accent w-3.5 h-3.5"
                />
                <span className="font-medium">Dry run</span>
                <span className="text-[10px] text-ink-faint">(preview duplicates only)</span>
              </label>

              {/* Match Columns */}
              {columns?.length > 0 && (
                <div>
                  <div className="flex items-center justify-between mb-1">
                    <label className="text-xs text-ink-muted font-medium">Match columns</label>
                    <div className="flex items-center gap-1.5">
                      <button
                        onClick={() => patch({ matchColumns: [...columns] })}
                        className="text-[10px] font-medium text-accent hover:text-accent-hover transition-colors cursor-pointer"
                      >
                        Select all
                      </button>
                      <span className="text-ink-faint text-[10px]">·</span>
                      <button
                        onClick={() => patch({ matchColumns: null })}
                        className="text-[10px] font-medium text-ink-faint hover:text-ink-muted transition-colors cursor-pointer"
                      >
                        Clear
                      </button>
                    </div>
                  </div>
                  <p className="text-[10px] text-ink-faint mb-1.5">
                    {opts.matchColumns?.length
                      ? `${opts.matchColumns.length} of ${columns.length} selected`
                      : 'None selected — will match on all columns'}
                  </p>
                  <div className="flex flex-wrap gap-1 max-h-[120px] overflow-y-auto rounded-lg border border-border bg-surface-raised/50 p-2">
                    {columns.map(col => {
                      const selected = opts.matchColumns?.includes(col) ?? false
                      return (
                        <button
                          key={col}
                          onClick={() => {
                            const cur = opts.matchColumns ?? []
                            const next = selected ? cur.filter(c => c !== col) : [...cur, col]
                            patch({ matchColumns: next.length ? next : null })
                          }}
                          className={`px-1.5 py-0.5 rounded text-[10px] font-mono border transition-colors cursor-pointer ${
                            selected
                              ? 'bg-accent/10 text-accent border-accent/30'
                              : 'bg-surface text-ink-faint border-border hover:border-ink-faint'
                          }`}
                        >
                          {col}
                        </button>
                      )
                    })}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-2.5 px-5 py-3 border-t border-border bg-surface-raised">
          <button
            onClick={onClose}
            className="px-4 py-1.5 rounded-lg text-xs font-semibold text-ink-muted hover:text-ink bg-surface hover:bg-border border border-border transition-colors cursor-pointer"
          >
            Cancel
          </button>
          <button
            onClick={onClose}
            className="px-4 py-1.5 rounded-lg text-xs font-semibold text-white bg-accent hover:bg-accent-hover transition-colors cursor-pointer shadow-sm"
          >
            Apply
          </button>
        </div>
      </div>
    </div>
  )
}

// ──────────────────────────────────────────────────
// Step Indicator — horizontal tab bar
// ──────────────────────────────────────────────────

const STEP_DEFS = [
  { id: 'import',    label: 'Import',    num: 1 },
  { id: 'clean',     label: 'Clean',     num: 2 },
  { id: 'normalize', label: 'Normalize', num: 3 },
  { id: 'dedup',     label: 'Dedup',     num: 4 },
]

function StepIndicator({ steps, viewStep, setViewStep }) {
  return (
    <div className="flex items-center border-b border-border bg-surface-raised">
      {STEP_DEFS.map((def, i) => {
        const step = steps[def.id]
        const isView    = viewStep === i
        const isDone    = step.status === 'done'
        const isSkipped = step.status === 'skipped'
        const isRunning = step.status === 'running'
        const isError   = step.status === 'error'
        const isPreview = step.status === 'preview'
        const canClick  = isDone || isSkipped || isView || isRunning || isError || isPreview

        return (
          <button
            key={def.id}
            onClick={() => canClick && setViewStep(i)}
            disabled={!canClick}
            className={`flex items-center gap-1.5 px-4 py-2.5 text-[11px] font-semibold border-b-2 transition-colors cursor-pointer disabled:cursor-default ${
              isView
                ? 'border-accent text-accent'
                : isDone || isSkipped
                ? 'border-transparent text-ink-muted hover:text-ink'
                : 'border-transparent text-ink-faint'
            }`}
          >
            <span className={`flex items-center justify-center w-4 h-4 rounded-full text-[9px] font-bold leading-none ${
              isDone    ? 'bg-done text-white'    :
              isSkipped ? 'bg-border text-ink-faint' :
              isRunning ? 'bg-accent text-white'  :
              isPreview ? 'bg-amber-400 text-white' :
              isError   ? 'bg-failed text-white'  :
              isView    ? 'bg-accent text-white'  :
                          'bg-border text-ink-faint'
            }`}>
              {isDone ? '✓' : isSkipped ? '–' : def.num}
            </span>
            {def.label}
          </button>
        )
      })}
    </div>
  )
}

// ──────────────────────────────────────────────────
// Column Stats Table
// ──────────────────────────────────────────────────

function ColStatsTable({ columnStats }) {
  if (!columnStats || Object.keys(columnStats).length === 0) return null
  const entries = Object.entries(columnStats)
  return (
    <div>
      <p className="text-[10px] font-semibold text-ink-muted uppercase tracking-wider mb-2">Column stats after transform</p>
      <div className="rounded-lg border border-border overflow-hidden">
        <table className="w-full text-[11px]">
          <thead>
            <tr className="bg-surface-raised border-b border-border">
              <th className="text-left px-2.5 py-1.5 font-semibold text-ink-muted">Column</th>
              <th className="text-right px-2.5 py-1.5 font-semibold text-ink-muted">Nulls</th>
              <th className="text-right px-2.5 py-1.5 font-semibold text-ink-muted">Unique</th>
              <th className="text-right px-2.5 py-1.5 font-semibold text-ink-muted">Min</th>
              <th className="text-right px-2.5 py-1.5 font-semibold text-ink-muted">Max</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(([col, stat], i) => (
              <tr key={col} className={i % 2 === 0 ? '' : 'bg-surface-raised/40'}>
                <td className="px-2.5 py-1 font-mono text-ink truncate max-w-[100px]" title={col}>{col}</td>
                <td className="px-2.5 py-1 text-right font-mono text-ink-muted">{stat.null_count ?? '—'}</td>
                <td className="px-2.5 py-1 text-right font-mono text-ink-muted">{stat.unique_count ?? '—'}</td>
                <td className="px-2.5 py-1 text-right font-mono text-ink-faint truncate max-w-[60px]" title={String(stat.min_value ?? '')}>{stat.min_value ?? '—'}</td>
                <td className="px-2.5 py-1 text-right font-mono text-ink-faint truncate max-w-[60px]" title={String(stat.max_value ?? '')}>{stat.max_value ?? '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ──────────────────────────────────────────────────
// Download Button
// ──────────────────────────────────────────────────

function DownloadBtn({ datasetId, dryRun }) {
  if (!datasetId) return null
  return (
    <a
      href={`${BACKEND}/api/v1/datasets/${datasetId}/export`}
      target="_blank"
      rel="noopener noreferrer"
      className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-surface-raised hover:bg-border border border-border text-ink text-[11px] font-semibold transition-colors cursor-pointer shadow-sm"
    >
      <Download size={12} strokeWidth={2.5} />
      {dryRun ? 'Download Original CSV' : 'Download CSV'}
    </a>
  )
}

// ──────────────────────────────────────────────────
// Duplicates Preview Dialog
// ──────────────────────────────────────────────────

function DuplicatesPreviewDialog({ open, onClose, rows, columns, totalDuplicates, duplicatesDatasetId }) {
  if (!open || !rows?.length) return null

  // Columns: _row_number first, then the rest
  const cols = columns?.length
    ? ['_row_number', ...columns]
    : Object.keys(rows[0] ?? {})

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/40 backdrop-blur-sm" onClick={onClose} />

      {/* Dialog */}
      <div className="relative w-full max-w-5xl mx-4 rounded-xl border border-border bg-surface shadow-2xl overflow-hidden flex flex-col" style={{ maxHeight: '85vh' }}>
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-border bg-surface-raised shrink-0">
          <div className="flex items-center gap-2.5">
            <Eye size={14} className="text-accent" strokeWidth={2.5} />
            <span className="text-sm font-semibold text-ink">Duplicate Rows Preview</span>
            <span className="text-[10px] font-mono text-ink-faint bg-surface px-2 py-0.5 rounded border border-border">
              {rows.length === totalDuplicates
                ? `${totalDuplicates} rows`
                : `Showing ${rows.length} of ${totalDuplicates}`}
            </span>
          </div>
          <div className="flex items-center gap-2">
            {duplicatesDatasetId && (
              <a
                href={`${BACKEND}/api/v1/datasets/${duplicatesDatasetId}/export`}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1.5 px-3 py-1 rounded-lg bg-accent hover:bg-accent-hover text-white text-[11px] font-semibold transition-colors cursor-pointer shadow-sm"
              >
                <FileDown size={12} strokeWidth={2.5} />
                Download All Duplicates CSV
              </a>
            )}
            <button onClick={onClose} className="text-ink-faint hover:text-ink transition-colors cursor-pointer p-1 rounded hover:bg-border">
              <X size={14} strokeWidth={2.5} />
            </button>
          </div>
        </div>

        {/* Table */}
        <div className="overflow-auto flex-1">
          <table className="w-full text-[11px] border-collapse">
            <thead className="sticky top-0 z-10">
              <tr className="bg-surface-raised border-b border-border">
                {cols.map(col => (
                  <th
                    key={col}
                    className={`px-3 py-2 text-left font-semibold uppercase tracking-wider whitespace-nowrap border-r border-border last:border-r-0 ${
                      col === '_row_number'
                        ? 'text-accent bg-accent/5 w-[70px]'
                        : 'text-ink-muted'
                    }`}
                  >
                    {col === '_row_number' ? 'Row #' : col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr
                  key={i}
                  className={`border-b border-border/50 hover:bg-accent/3 transition-colors ${
                    i % 2 === 0 ? 'bg-surface' : 'bg-surface-raised/30'
                  }`}
                >
                  {cols.map(col => (
                    <td
                      key={col}
                      className={`px-3 py-1.5 font-mono border-r border-border/30 last:border-r-0 max-w-[200px] truncate ${
                        col === '_row_number'
                          ? 'text-accent font-semibold text-center'
                          : 'text-ink-muted'
                      }`}
                      title={row[col] ?? ''}
                    >
                      {row[col] ?? ''}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-5 py-2.5 border-t border-border bg-surface-raised shrink-0">
          <p className="text-[10px] text-ink-faint">
            {totalDuplicates} duplicate{totalDuplicates !== 1 ? 's' : ''} found
            {rows.length < totalDuplicates && ` · Preview limited to first ${rows.length} rows`}
          </p>
          <button
            onClick={onClose}
            className="px-4 py-1.5 rounded-lg text-xs font-semibold text-ink-muted hover:text-ink bg-surface hover:bg-border border border-border transition-colors cursor-pointer"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  )
}

// ──────────────────────────────────────────────────
// Import Step Card
// ──────────────────────────────────────────────────

function ImportStepCard({ uploadResult, onProceed }) {
  const analysis = uploadResult.analysis
  const columns  = uploadResult.columns ?? []

  const colEntries = columns.map(name => {
    const info  = analysis?.columns?.[name]
    const total = uploadResult.rows || 1
    const nulls = info?.null_count ?? 0
    return {
      name,
      type: info?.inferred_type ?? 'free_text',
      fill: total > 0 ? (total - nulls) / total : 1,
      nulls,
    }
  })

  return (
    <div className="flex flex-col gap-4 p-4">
      {/* Summary badges */}
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-done-bg border border-done">
          <CheckCircle2 size={12} className="text-done" strokeWidth={2.5} />
          <span className="text-[11px] font-semibold text-done">Imported</span>
        </div>
        <span className="text-[11px] font-mono text-ink-muted">
          <span className="font-semibold text-ink">{uploadResult.rows?.toLocaleString()}</span> rows
        </span>
        <span className="text-[11px] font-mono text-ink-muted">
          <span className="font-semibold text-ink">{columns.length}</span> cols
        </span>
        <span className="text-[11px] font-mono text-ink-faint truncate max-w-[140px]" title={uploadResult.filename}>
          {uploadResult.filename}
        </span>
      </div>

      {/* Quality */}
      <div>
        <div className="flex items-center justify-between mb-1">
          <span className="text-[10px] font-semibold text-ink-muted uppercase tracking-wider">Data quality</span>
          <span className="text-[10px] font-mono text-ink-faint">avg fill</span>
        </div>
        <QualityBar value={analysis?.overall_quality} />
      </div>

      {/* Columns */}
      <div>
        <p className="text-[10px] font-semibold text-ink-muted uppercase tracking-wider mb-2">Columns detected</p>
        <div className="flex flex-col gap-1.5 max-h-[200px] overflow-y-auto pr-1">
          {colEntries.map(({ name, type, fill, nulls }) => (
            <div key={name} className="flex items-center gap-2">
              <span className={`shrink-0 inline-block px-1.5 py-0.5 rounded border text-[10px] font-mono leading-tight ${TYPE_COLOR[type] ?? TYPE_COLOR.free_text}`}>
                {type}
              </span>
              <span className="flex-1 text-xs text-ink truncate font-medium" title={name}>{name}</span>
              <div className="shrink-0 flex items-center gap-1.5 w-20">
                <div className="flex-1 h-1 rounded-full bg-surface-raised overflow-hidden">
                  <div
                    className={`h-full rounded-full ${fill >= 0.8 ? 'bg-done' : fill >= 0.5 ? 'bg-yellow-400' : 'bg-failed'}`}
                    style={{ width: `${Math.round(fill * 100)}%` }}
                  />
                </div>
                {nulls > 0 && <span className="text-[10px] font-mono text-ink-faint shrink-0">{nulls}✗</span>}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Dataset ID */}
      {/* <p className="text-[10px] text-ink-faint font-mono truncate" title={uploadResult.dataset_id}>
        dataset_id: {uploadResult.dataset_id}
      </p> */}

      {/* Proceed */}
      <div className="pt-1">
        <button
          onClick={onProceed}
          className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-accent hover:bg-accent-hover text-white text-xs font-semibold transition-colors cursor-pointer shadow-sm"
        >
          Proceed to Clean
          <ChevronRight size={13} strokeWidth={2.5} />
        </button>
      </div>
    </div>
  )
}

// ──────────────────────────────────────────────────
// Normalize Report Card — value-level transform breakdown
// ──────────────────────────────────────────────────

function NormalizeReportCard({ report }) {
  const [open, setOpen] = useState(false)
  const [tablesOpen, setTablesOpen] = useState(false)
  const vl = report?.value_level
  const OPS_PAGE = 20
  const [visibleOps, setVisibleOps] = useState(OPS_PAGE)

  // Show nothing only if there's literally no report at all
  if (!report) return null

  const allOps = vl?.operations ?? []
  const shownOps = allOps.slice(0, visibleOps)
  const totalModified = vl?.total_cells_modified ?? 0
  const tables = report?.tables ?? []
  const relationships = report?.relationships ?? []
  const multiValueSplits = report?.multi_value_splits ?? []
  const hasDbNorm = (report?.normal_form ?? 0) > 0
  const hasValueLevel = allOps.length > 0

  const OP_LABELS = {
    min_max_scale: 'Min-Max Scale',
    z_score: 'Z-Score',
    email_normalize: 'Email Normalize',
    phone_format: 'Phone Format',
    date_format: 'Date Format',
    enum_map: 'Enum Map',
    url_normalize: 'URL Normalize',
    to_lowercase: 'Lowercase',
    to_uppercase: 'Uppercase',
    trim: 'Trim',
    currency_format: 'Currency Format',
    unit_convert: 'Unit Convert',
  }

  // Collect all table dataset IDs for ZIP download
  const tableIds = tables.map(t => t.dataset_id).filter(Boolean)

  const handleDownloadZip = async () => {
    if (tableIds.length === 0) return
    try {
      const resp = await fetch(`${BACKEND}/api/v1/datasets/export-zip?ids=${tableIds.join(',')}`)
      if (!resp.ok) throw new Error('Failed to download')
      const blob = await resp.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `normalized_tables_${report.normal_form}NF.zip`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    } catch (err) {
      console.error('ZIP download failed:', err)
    }
  }

  // Summary line
  const summaryParts = []
  if (hasValueLevel) summaryParts.push(`${totalModified.toLocaleString()} cells modified · ${allOps.length} op${allOps.length !== 1 ? 's' : ''}`)
  if (hasDbNorm) summaryParts.push(`${report.normal_form}NF · ${tables.length} table${tables.length !== 1 ? 's' : ''}`)

  return (
    <div className="rounded-lg border border-border bg-surface-raised overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-3 py-2 hover:bg-border/50 transition-colors cursor-pointer"
      >
        <div className="flex items-center gap-2">
          <Layers size={13} className="text-accent" strokeWidth={2} />
          <span className="text-[11px] font-semibold text-ink">Normalize Report</span>
          <span className="text-[9px] font-mono text-ink-faint">
            {summaryParts.join(' | ')}
          </span>
        </div>
        <ChevronDown size={13} className={`text-ink-faint transition-transform ${open ? 'rotate-180' : ''}`} strokeWidth={2} />
      </button>

      {open && (
        <div className="border-t border-border">
          {/* ── Value-level operations table ── */}
          {hasValueLevel && (
            <div className="max-h-[300px] overflow-y-auto">
              <div className="px-3 py-1.5 bg-surface border-b border-border">
                <span className="text-[10px] font-semibold text-ink-muted uppercase tracking-wider">Value-Level Transforms</span>
              </div>
              <table className="w-full text-[10px]">
                <thead>
                  <tr className="border-b border-border bg-surface">
                    <th className="text-left px-3 py-1.5 font-semibold text-ink-muted uppercase tracking-wider">Operation</th>
                    <th className="text-left px-3 py-1.5 font-semibold text-ink-muted uppercase tracking-wider">Column</th>
                    <th className="text-right px-3 py-1.5 font-semibold text-ink-muted uppercase tracking-wider">Cells</th>
                    <th className="text-left px-3 py-1.5 font-semibold text-ink-muted uppercase tracking-wider">Before → After</th>
                  </tr>
                </thead>
                <tbody>
                  {shownOps.map((op, i) => (
                    <tr key={i} className="border-b border-border/50">
                      <td className="px-3 py-1.5 font-mono text-ink">
                        <span className="inline-block px-1.5 py-0.5 rounded text-[9px] font-semibold bg-violet-50 text-violet-700 border border-violet-200">
                          {OP_LABELS[op.operation] ?? op.operation}
                        </span>
                      </td>
                      <td className="px-3 py-1.5 font-mono text-ink-muted">{op.column || '—'}</td>
                      <td className="px-3 py-1.5 font-mono text-ink text-right font-semibold">{op.cells_affected?.toLocaleString()}</td>
                      <td className="px-3 py-1.5">
                        {op.sample_before?.length > 0 ? (
                          <div className="space-y-0.5">
                            {op.sample_before.slice(0, 3).map((bv, j) => (
                              <div key={j} className="flex items-center gap-1 text-[9px] font-mono">
                                <span className="text-red-500 line-through truncate max-w-[80px]" title={bv}>{bv || '(empty)'}</span>
                                <span className="text-ink-faint">→</span>
                                <span className="text-emerald-600 truncate max-w-[80px]" title={op.sample_after?.[j]}>{op.sample_after?.[j] || '(empty)'}</span>
                              </div>
                            ))}
                            {op.sample_before.length > 3 && (
                              <span className="text-[8px] text-ink-faint">+{op.sample_before.length - 3} more…</span>
                            )}
                          </div>
                        ) : (
                          <span className="text-ink-faint">—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {visibleOps < allOps.length && (
                <div className="px-3 py-2 text-center border-t border-border">
                  <button
                    onClick={() => setVisibleOps(c => Math.min(c + OPS_PAGE, allOps.length))}
                    className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-surface hover:bg-border/60 border border-border text-ink-muted text-[10px] font-semibold transition-colors cursor-pointer"
                  >
                    Load More ({allOps.length - visibleOps} remaining)
                  </button>
                </div>
              )}
            </div>
          )}

          {/* ── DB Normalization Report ── */}
          {hasDbNorm && (
            <div className="border-t border-border">
              <div className="px-3 py-2 bg-blue-50/40 border-b border-border">
                <div className="flex items-center gap-2">
                  <Database size={13} className="text-blue-600" strokeWidth={2} />
                  <span className="text-[10px] font-semibold text-ink uppercase tracking-wider">
                    Database Normalization — {report.normal_form}NF
                  </span>
                </div>
                {report.description && (
                  <p className="text-[9px] text-ink-muted mt-1">{report.description}</p>
                )}
                {report.primary_key && (
                  <p className="text-[9px] text-ink-faint mt-0.5">Primary key: <span className="font-mono font-semibold text-blue-700">{report.primary_key}</span></p>
                )}
              </div>

              {/* Multi-value splits (1NF) */}
              {multiValueSplits.length > 0 && (
                <div className="px-3 py-2 border-b border-border">
                  <div className="flex items-center gap-2 mb-1.5">
                    <Split size={12} className="text-orange-500" strokeWidth={2} />
                    <span className="text-[10px] font-semibold text-ink">Multi-Value Splits (1NF)</span>
                  </div>
                  <div className="space-y-1.5">
                    {multiValueSplits.map((mv, i) => (
                      <div key={i} className="rounded border border-orange-200 bg-orange-50/50 px-2.5 py-1.5">
                        <div className="flex items-center gap-2 text-[10px]">
                          <span className="font-mono font-semibold text-orange-700">{mv.column}</span>
                          <span className="text-ink-faint">·</span>
                          <span className="text-ink-muted">{mv.cells_split} cells split</span>
                          <span className="text-ink-faint">·</span>
                          <span className="text-ink-muted">{mv.rows_before} → {mv.rows_after} rows</span>
                        </div>
                        {mv.sample_before && (
                          <div className="flex items-center gap-1 text-[9px] font-mono mt-1">
                            <span className="text-red-500 line-through truncate max-w-[120px]" title={mv.sample_before}>{mv.sample_before}</span>
                            <span className="text-ink-faint">→</span>
                            <span className="text-emerald-600 truncate max-w-[120px]" title={mv.sample_after}>{mv.sample_after}</span>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Decomposed Tables */}
              {tables.length > 0 && (
                <div className="px-3 py-2 border-b border-border">
                  <button
                    onClick={() => setTablesOpen(!tablesOpen)}
                    className="w-full flex items-center justify-between cursor-pointer"
                  >
                    <div className="flex items-center gap-2">
                      <Table2 size={12} className="text-blue-600" strokeWidth={2} />
                      <span className="text-[10px] font-semibold text-ink">Decomposed Tables</span>
                      <span className="text-[9px] font-mono text-ink-faint bg-surface px-1.5 py-0.5 rounded border border-border">
                        {tables.length} table{tables.length !== 1 ? 's' : ''}
                      </span>
                    </div>
                    <ChevronDown size={12} className={`text-ink-faint transition-transform ${tablesOpen ? 'rotate-180' : ''}`} strokeWidth={2} />
                  </button>

                  {tablesOpen && (
                    <div className="mt-2 space-y-1.5">
                      {tables.map((tbl, i) => (
                        <div key={i} className={`rounded-lg border px-2.5 py-2 ${tbl.is_main ? 'border-blue-300 bg-blue-50/50' : 'border-border bg-surface-raised'}`}>
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                              {tbl.is_main ? (
                                <Database size={11} className="text-blue-600" strokeWidth={2} />
                              ) : (
                                <Table2 size={11} className="text-ink-faint" strokeWidth={2} />
                              )}
                              <span className="text-[10px] font-semibold font-mono text-ink">{tbl.name}</span>
                              {tbl.is_main && (
                                <span className="px-1.5 py-0.5 rounded text-[8px] font-semibold bg-blue-100 text-blue-700 border border-blue-200">MAIN</span>
                              )}
                            </div>
                            <span className="text-[9px] font-mono text-ink-faint">{tbl.record_count?.toLocaleString()} rows</span>
                          </div>
                          {tbl.description && (
                            <p className="text-[9px] text-ink-muted mt-0.5">{tbl.description}</p>
                          )}
                          <div className="flex flex-wrap gap-1 mt-1">
                            {(tbl.columns ?? []).map(col => (
                              <span key={col} className="px-1 py-0.5 rounded text-[8px] font-mono bg-surface text-ink-muted border border-border">
                                {col}
                              </span>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {/* Relationships */}
              {relationships.length > 0 && (
                <div className="px-3 py-2 border-b border-border">
                  <div className="flex items-center gap-2 mb-1.5">
                    <Link size={12} className="text-emerald-600" strokeWidth={2} />
                    <span className="text-[10px] font-semibold text-ink">Relationships</span>
                  </div>
                  <div className="space-y-1">
                    {relationships.map((rel, i) => (
                      <div key={i} className="flex items-center gap-1.5 text-[9px] font-mono">
                        <span className="px-1.5 py-0.5 rounded bg-blue-50 text-blue-700 border border-blue-200 font-semibold">{rel.from_table}</span>
                        <span className="text-ink-faint">.{rel.from_column}</span>
                        <span className="text-ink-faint">→</span>
                        <span className="px-1.5 py-0.5 rounded bg-emerald-50 text-emerald-700 border border-emerald-200 font-semibold">{rel.to_table}</span>
                        <span className="text-ink-faint">.{rel.to_column}</span>
                        <span className="text-[8px] text-ink-faint italic ml-1">({rel.type})</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* ZIP download */}
              {tableIds.length > 1 && (
                <div className="px-3 py-2">
                  <button
                    onClick={handleDownloadZip}
                    className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-700 text-white text-[10px] font-semibold transition-colors cursor-pointer shadow-sm"
                  >
                    <Archive size={12} strokeWidth={2} />
                    Download All Tables (ZIP)
                  </button>
                  <p className="text-[8px] text-ink-faint mt-1">Contains {tableIds.length} CSV files — one per decomposed table.</p>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ──────────────────────────────────────────────────
// Dedup Report Card — detailed dedup breakdown
// ──────────────────────────────────────────────────

function DedupReportCard({ report }) {
  const [open, setOpen] = useState(false)
  const [expandedGroup, setExpandedGroup] = useState(null)
  const PAGE_SIZE = 20
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE)
  const allGroups = report.groups ?? []
  const shownGroups = allGroups.slice(0, visibleCount)

  return (
    <div className="rounded-lg border border-border bg-surface-raised overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-3 py-2 hover:bg-border/50 transition-colors cursor-pointer"
      >
        <div className="flex items-center gap-2">
          <Database size={13} className="text-violet-600" strokeWidth={2} />
          <span className="text-[11px] font-semibold text-ink">Dedup Report</span>
          <span className="text-[9px] font-mono text-ink-faint">
            {report.total_groups} group{report.total_groups !== 1 ? 's' : ''} · {report.total_duplicates} duplicate{report.total_duplicates !== 1 ? 's' : ''} removed · {report.total_kept} rows kept
          </span>
        </div>
        <ChevronDown size={13} className={`text-ink-faint transition-transform ${open ? 'rotate-180' : ''}`} strokeWidth={2} />
      </button>

      {open && (
        <div className="border-t border-border">
          {/* Strategy summary */}
          <div className="px-3 py-2 bg-surface border-b border-border flex flex-wrap items-center gap-2">
            <span className={`inline-flex items-center px-2 py-0.5 rounded text-[9px] font-semibold border ${
              report.strategy === 'fuzzy'
                ? 'bg-amber-50 text-amber-700 border-amber-200'
                : 'bg-blue-50 text-blue-700 border-blue-200'
            }`}>
              {report.strategy === 'fuzzy' ? 'Fuzzy match' : 'Exact match'}
            </span>
            <span className="inline-flex items-center px-2 py-0.5 rounded text-[9px] font-semibold bg-surface-raised text-ink-muted border border-border">
              Keep: {report.keep_strategy}
            </span>
            {report.match_columns?.length > 0 && (
              <span className="text-[9px] text-ink-faint">
                on {report.match_columns.length} column{report.match_columns.length > 1 ? 's' : ''}
              </span>
            )}
          </div>

          {/* Group list */}
          <div className="max-h-[400px] overflow-y-auto divide-y divide-border/50">
            {shownGroups.map((group, i) => {
              const isExpanded = expandedGroup === i
              return (
                <div key={i} className="px-3 py-2">
                  <button
                    onClick={() => setExpandedGroup(isExpanded ? null : i)}
                    className="w-full flex items-center justify-between text-left cursor-pointer"
                  >
                    <div className="flex items-center gap-2 min-w-0">
                      <span className="text-[9px] font-mono text-ink-faint shrink-0">#{i + 1}</span>
                      <span className="text-[10px] font-mono text-ink truncate max-w-[200px]" title={group.match_key}>
                        {group.match_key.length > 50 ? group.match_key.slice(0, 50) + '…' : group.match_key}
                      </span>
                    </div>
                    <div className="flex items-center gap-2 shrink-0">
                      <span className="text-[9px] font-mono text-ink-muted">
                        {group.group_size} rows → kept row #{group.kept_index}
                      </span>
                      <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[9px] font-semibold bg-amber-50 text-amber-700 border border-amber-200">
                        -{group.group_size - 1} dropped
                      </span>
                      <ChevronDown size={10} className={`text-ink-faint transition-transform ${isExpanded ? 'rotate-180' : ''}`} strokeWidth={2} />
                    </div>
                  </button>

                  {isExpanded && group.dropped_rows?.length > 0 && (
                    <div className="mt-2 ml-4 rounded border border-border bg-surface overflow-x-auto">
                      <table className="w-full text-[9px]">
                        <thead>
                          <tr className="border-b border-border bg-surface-raised">
                            <th className="text-left px-2 py-1 font-semibold text-ink-muted">Row</th>
                            {Object.keys(group.dropped_rows[0]).filter(k => k !== '_row_number').slice(0, 6).map(col => (
                              <th key={col} className="text-left px-2 py-1 font-semibold text-ink-muted truncate max-w-[80px]">{col}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {group.dropped_rows.map((row, ri) => (
                            <tr key={ri} className="border-b border-border/30 bg-amber-50/30">
                              <td className="px-2 py-1 font-mono text-ink-faint">#{row._row_number}</td>
                              {Object.keys(row).filter(k => k !== '_row_number').slice(0, 6).map(col => (
                                <td key={col} className="px-2 py-1 font-mono text-ink truncate max-w-[80px]" title={row[col]}>
                                  {row[col]?.length > 20 ? row[col].slice(0, 20) + '…' : (row[col] || '—')}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                      {group.group_size - 1 > group.dropped_rows.length && (
                        <p className="text-[8px] text-ink-faint px-2 py-1">
                          +{group.group_size - 1 - group.dropped_rows.length} more dropped row(s)
                        </p>
                      )}
                    </div>
                  )}
                </div>
              )
            })}
            {visibleCount < allGroups.length && (
              <div className="px-3 py-2 text-center">
                <button
                  onClick={() => setVisibleCount(c => Math.min(c + PAGE_SIZE, allGroups.length))}
                  className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-surface hover:bg-border/60 border border-border text-ink-muted text-[10px] font-semibold transition-colors cursor-pointer"
                >
                  Load More ({allGroups.length - visibleCount} remaining)
                </button>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

// ──────────────────────────────────────────────────
// ETL Step Card — reusable for clean / normalize / dedup
// ──────────────────────────────────────────────────

function ETLStepCard({ step, label, nextLabel, showDuplicates, allColumns, prevQuality, normInfo, onRun, onProceed }) {
  const isIdle    = step.status === 'idle'
  const isPreview = step.status === 'preview'
  const isRunning = step.status === 'running'
  const isDone    = step.status === 'done'
  const isError   = step.status === 'error'
  const isSkipped = step.status === 'skipped'
  const r = step.result
  const p = step.params  // dedup job params (strategy, dry_run, match_columns, etc.)
  const [dupePreviewOpen, setDupePreviewOpen] = useState(false)
  const [reportOpen, setReportOpen] = useState(false)
  const OPS_PAGE = 20
  const [visibleOps, setVisibleOps] = useState(OPS_PAGE)
  const allOps = r?.clean_report?.operations ?? []
  const shownOps = allOps.slice(0, visibleOps)

  // DB normalization local config (for normalize step preview)
  const [dbNormForm, setDbNormForm] = useState(0)
  const [dbPkCol, setDbPkCol]       = useState('')
  const [dbCatCols, setDbCatCols]   = useState([])

  useEffect(() => {
    if (isPreview && normInfo?.applicable) {
      if (normInfo.potentialPKs?.length > 0 && !dbPkCol) setDbPkCol(normInfo.potentialPKs[0])
      if (normInfo.categoricalCols?.length > 0 && dbCatCols.length === 0) setDbCatCols([...normInfo.categoricalCols])
      if (dbNormForm === 0) setDbNormForm(normInfo.categoricalCols?.length > 0 && normInfo.potentialPKs?.length > 0 ? 2 : 1)
    }
  }, [isPreview, normInfo])

  if (isSkipped) {
    return (
      <div className="flex flex-col gap-3 p-4">
        <div className="flex items-center gap-2 text-ink-faint">
          <span className="text-sm font-medium">Skipped</span>
          <span className="text-[11px]">— no rules needed by the analyzer</span>
        </div>
        {onProceed && (
          <button onClick={onProceed} className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-accent hover:bg-accent-hover text-white text-xs font-semibold transition-colors cursor-pointer shadow-sm w-fit">
            {nextLabel ?? 'Continue'}
            <ChevronRight size={13} strokeWidth={2.5} />
          </button>
        )}
      </div>
    )
  }

  if (isIdle) {
    return (
      <div className="flex flex-col gap-3 p-4">
        <p className="text-sm text-ink-muted">Ready to run <span className="font-semibold text-ink">{label}</span> stage.</p>
        <button onClick={onRun} className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-accent hover:bg-accent-hover text-white text-xs font-semibold transition-colors cursor-pointer shadow-sm w-fit">
          Run {label}
          <ChevronRight size={13} strokeWidth={2.5} />
        </button>
      </div>
    )
  }

  // ── Preview state (Clean only) — show rules before executing ──
  if (isPreview && step.cleanRules) {
    const rules = step.cleanRules
    // Group rules by operation for readability
    const grouped = {}
    for (const rule of rules) {
      const key = rule.operation
      if (!grouped[key]) grouped[key] = []
      grouped[key].push(rule)
    }
    const opLabels = {
      trim_whitespace: 'Trim whitespace',
      to_lowercase: 'Lowercase',
      to_uppercase: 'Uppercase',
      remove_html: 'Remove HTML',
      remove_newlines: 'Remove newlines',
      collapse_whitespace: 'Collapse whitespace',
      regex_replace: 'Regex replace',
      fill_null: 'Fill missing values',
      drop_null: 'Drop rows with nulls',
      drop_empty_columns: 'Drop empty columns',
      remove_special_chars: 'Remove special chars',
      standardize_date: 'Standardize dates',
      type_cast: 'Type cast',
      fix_mismatched_types: 'Fix mismatched types',
      fix_categorical_outliers: 'Fix outliers',
    }
    const hasDestructive = rules.some(r => r.operation === 'drop_null' || r.operation === 'drop_empty_columns')

    return (
      <div className="flex flex-col gap-4 p-4">
        <div>
          <h4 className="text-sm font-semibold text-ink mb-1">Clean Preview</h4>
          <p className="text-[11px] text-ink-muted">
            The following <span className="font-semibold">{rules.length}</span> operations will be applied to your data.
            Review &amp; confirm before running.
          </p>
        </div>

        {/* {hasDestructive && (
          <div className="flex items-start gap-2 px-3 py-2 rounded-lg bg-amber-50 border border-amber-200">
            <AlertCircle size={14} className="text-amber-600 mt-0.5 shrink-0" strokeWidth={2} />
            <p className="text-[11px] text-amber-800">
              <span className="font-semibold">Destructive operations detected.</span> Rows or columns may be removed.
            </p>
          </div>
        )} */}

        <div className="space-y-2 max-h-[350px] overflow-y-auto">
          {Object.entries(grouped).map(([op, opRules]) => {
            const isDestructive = op === 'drop_null' || op === 'drop_empty_columns'
            return (
              <div key={op} className={`rounded-lg border px-3 py-2 ${isDestructive ? 'border-amber-200 bg-amber-50/50' : 'border-border bg-surface-raised'}`}>
                <div className="flex items-center gap-2 mb-1">
                  <span className={`text-[11px] font-semibold ${isDestructive ? 'text-amber-700' : 'text-ink'}`}>
                    {opLabels[op] ?? op}
                  </span>
                  <span className="text-[9px] font-mono text-ink-faint bg-surface px-1.5 py-0.5 rounded border border-border">
                    {opRules.length === 1 && opRules[0].column === '' ? 'all columns' : `${opRules.length} col${opRules.length > 1 ? 's' : ''}`}
                  </span>
                </div>
                <div className="flex flex-wrap gap-1">
                  {opRules.map((rule, i) => (
                    rule.column ? (
                      <span key={i} className="px-1.5 py-0.5 rounded text-[9px] font-mono bg-accent/8 text-accent border border-accent/20">
                        {rule.column}
                      </span>
                    ) : null
                  ))}
                </div>
                {opRules[0]?.params?.strategy && (
                  <p className="text-[9px] text-ink-faint mt-1">Strategy: {opRules[0].params.strategy}</p>
                )}
              </div>
            )
          })}
        </div>

        <div className="flex items-center gap-3 pt-1">
          <button
            onClick={onRun}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-accent hover:bg-accent-hover text-white text-xs font-semibold transition-colors cursor-pointer shadow-sm"
          >
            Apply Cleaning
            <ChevronRight size={13} strokeWidth={2.5} />
          </button>
        </div>
      </div>
    )
  }

  // ── Preview state (Normalize) — show planned normalize rules + DB norm config before executing ──
  if (isPreview && normInfo?.applicable) {
    const NF_DESCRIPTIONS = {
      1: { label: '1NF — First Normal Form', desc: 'Eliminates repeating groups by splitting multi-valued cells (separated by ; or |) into individual rows. Ensures every cell contains a single atomic value.' },
      2: { label: '2NF — Second Normal Form', desc: 'Builds on 1NF. Extracts categorical columns into separate lookup tables with auto-generated IDs, replacing original values with foreign key references. Removes partial dependencies.' },
      3: { label: '3NF — Third Normal Form', desc: 'Builds on 2NF. Identifies and extracts transitive dependencies — columns that depend on non-key columns — into their own tables. Produces a fully normalized relational schema.' },
    }

    const toggleCatCol = (col) => {
      setDbCatCols(prev => prev.includes(col) ? prev.filter(c => c !== col) : [...prev, col])
    }

    return (
      <div className="flex flex-col gap-4 p-4">
        <div>
          <h4 className="text-sm font-semibold text-ink mb-1">Normalize Preview</h4>
          <p className="text-[11px] text-ink-muted">
            Configure database normalization to decompose your data into relational tables, then confirm.
          </p>
        </div>

        {/* ── Database Normalization config section ── */}
        <div className="rounded-lg border border-blue-200 bg-blue-50/30 p-3 space-y-3">
            <div className="flex items-center gap-2">
              <Database size={14} className="text-blue-600" strokeWidth={2} />
              <span className="text-[12px] font-semibold text-ink">Database Normalization</span>
            </div>
            <p className="text-[10px] text-ink-muted">
              Your data has <span className="font-semibold text-blue-700">{normInfo.categoricalCols?.length ?? 0}</span> categorical column{(normInfo.categoricalCols?.length ?? 0) !== 1 ? 's' : ''} and <span className="font-semibold text-blue-700">{normInfo.potentialPKs?.length ?? 0}</span> potential primary key{(normInfo.potentialPKs?.length ?? 0) !== 1 ? 's' : ''}. Decompose into relational tables to reduce redundancy.
            </p>

            {/* Normal Form Selector */}
            <div>
              <label className="text-[10px] font-semibold text-ink-muted uppercase tracking-wider mb-1 block">Normal Form</label>
              <div className="space-y-1.5">
                {[1, 2, 3].map(nf => (
                  <label key={nf} className={`flex items-start gap-2.5 rounded-lg border px-3 py-2 cursor-pointer transition-colors ${dbNormForm === nf ? 'border-blue-400 bg-blue-50' : 'border-border bg-surface-raised hover:bg-surface'}`}>
                    <input
                      type="radio"
                      name="normalForm"
                      checked={dbNormForm === nf}
                      onChange={() => setDbNormForm(nf)}
                      className="mt-0.5 accent-blue-600"
                    />
                    <div className="flex-1 min-w-0">
                      <span className="text-[11px] font-semibold text-ink">{NF_DESCRIPTIONS[nf].label}</span>
                      <p className="text-[9px] text-ink-muted mt-0.5 leading-relaxed">{NF_DESCRIPTIONS[nf].desc}</p>
                    </div>
                  </label>
                ))}
              </div>
            </div>

            {/* Primary Key Column Selector (for 2NF+) */}
            {dbNormForm >= 2 && normInfo.potentialPKs?.length > 0 && (
              <div>
                <label className="text-[10px] font-semibold text-ink-muted uppercase tracking-wider mb-1 block">Primary Key Column</label>
                <select
                  value={dbPkCol}
                  onChange={e => setDbPkCol(e.target.value)}
                  className="w-full text-[11px] font-mono rounded-lg border border-border bg-surface px-2.5 py-1.5 text-ink focus:border-blue-400 focus:ring-1 focus:ring-blue-400 outline-none"
                >
                  {normInfo.potentialPKs.map(pk => (
                    <option key={pk} value={pk}>{pk}</option>
                  ))}
                </select>
                <p className="text-[9px] text-ink-faint mt-1">Column with high uniqueness (&gt;90%) used as the primary key for decomposition.</p>
              </div>
            )}

            {/* Categorical Columns Selector (for 2NF+) */}
            {dbNormForm >= 2 && normInfo.categoricalCols?.length > 0 && (
              <div>
                <label className="text-[10px] font-semibold text-ink-muted uppercase tracking-wider mb-1 block">Categorical Columns to Decompose</label>
                <div className="flex flex-wrap gap-1.5 max-h-[120px] overflow-y-auto">
                  {normInfo.categoricalCols.map(col => (
                    <label key={col} className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-lg border cursor-pointer transition-colors text-[10px] font-mono ${dbCatCols.includes(col) ? 'border-blue-400 bg-blue-50 text-blue-700' : 'border-border bg-surface-raised text-ink-muted hover:bg-surface'}`}>
                      <input
                        type="checkbox"
                        checked={dbCatCols.includes(col)}
                        onChange={() => toggleCatCol(col)}
                        className="accent-blue-600 w-3 h-3"
                      />
                      {col}
                    </label>
                  ))}
                </div>
                <p className="text-[9px] text-ink-faint mt-1">Low-cardinality columns to extract into separate lookup tables.</p>
              </div>
            )}

            {/* 2NF+ validation warning */}
            {dbNormForm >= 2 && (normInfo.potentialPKs?.length === 0 || dbCatCols.length === 0) && (
              <div className="flex items-start gap-2 px-3 py-2 rounded-lg bg-amber-50 border border-amber-200">
                <AlertCircle size={13} className="text-amber-600 mt-0.5 shrink-0" strokeWidth={2} />
                <p className="text-[10px] text-amber-800">
                  {normInfo.potentialPKs?.length === 0
                    ? '2NF+ requires a primary key column. No high-uniqueness columns detected.'
                    : 'Select at least one categorical column to decompose for 2NF+.'}
                </p>
              </div>
            )}
          </div>

        {/* Apply button */}
        <div className="flex items-center gap-3 pt-1">
          <button
            onClick={() => {
              const dbConfig = dbNormForm > 0
                ? { normalForm: dbNormForm, primaryKeyColumn: dbPkCol, categoricalColumns: dbCatCols }
                : undefined
              onRun(dbConfig)
            }}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-accent hover:bg-accent-hover text-white text-xs font-semibold transition-colors cursor-pointer shadow-sm"
          >
            Apply Normalization
            <ChevronRight size={13} strokeWidth={2.5} />
          </button>
        </div>
      </div>
    )
  }

  if (isRunning) {
    return (
      <div className="flex flex-col gap-3 p-4">
        <div className="flex items-center gap-2">
          <Loader2 size={14} className="text-accent animate-spin" strokeWidth={2.5} />
          <span className="text-sm text-ink-muted">{label} in progress…</span>
        </div>
        {step.jobId && <p className="text-[10px] font-mono text-ink-faint">job: {step.jobId}</p>}
      </div>
    )
  }

  if (isError) {
    return (
      <div className="flex flex-col gap-3 p-4">
        <div className="flex items-center gap-2 text-failed">
          <AlertCircle size={14} strokeWidth={2} />
          <span className="text-sm font-medium">{label} failed</span>
        </div>
        <p className="text-xs font-mono text-ink-muted break-all">{step.error}</p>
        <button onClick={onRun} className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-surface-raised hover:bg-border border border-border text-ink text-[11px] font-semibold transition-colors cursor-pointer w-fit">
          Retry
        </button>
      </div>
    )
  }

  // ── Done state ──
  return (
    <div className="flex flex-col gap-4 p-4">
      {/* Metric cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5">
        {[
          { lbl: 'Processed', val: r?.processed?.toLocaleString() ?? '—', accent: true },
          { lbl: 'Skipped',   val: r?.skipped?.toLocaleString() ?? '—' },
          { lbl: 'Failed',    val: r?.failed?.toLocaleString() ?? '—', warn: (r?.failed ?? 0) > 0 },
          { lbl: showDuplicates ? 'Duplicates' : 'Duration', val: showDuplicates ? (r?.duplicates_found?.toLocaleString() ?? '—') : (r?.duration ?? '—') },
        ].map(({ lbl, val, accent, warn }) => (
          <div key={lbl} className="rounded-lg border border-border bg-surface-raised px-2.5 py-2 text-center">
            <p className={`text-base font-bold font-mono ${accent ? 'text-accent' : warn ? 'text-failed' : 'text-ink'}`}>{val}</p>
            <p className="text-[9px] text-ink-faint uppercase tracking-wider mt-0.5">{lbl}</p>
          </div>
        ))}
      </div>

      {/* ── Detailed Clean Report ── */}
      {r?.clean_report && r.clean_report.operations?.length > 0 && (
        <div className="rounded-lg border border-border bg-surface-raised overflow-hidden">
          <button
            onClick={() => setReportOpen(!reportOpen)}
            className="w-full flex items-center justify-between px-3 py-2 hover:bg-border/50 transition-colors cursor-pointer"
          >
            <div className="flex items-center gap-2">
              <FileText size={13} className="text-accent" strokeWidth={2} />
              <span className="text-[11px] font-semibold text-ink">Cleaning Report</span>
              <span className="text-[9px] font-mono text-ink-faint">
                {r.clean_report.total_cells_modified?.toLocaleString()} cells modified
                {r.clean_report.total_rows_dropped > 0 && (
                  <span className="text-amber-600 ml-1">· {r.clean_report.total_rows_dropped} rows dropped</span>
                )}
                {r.clean_report.columns_dropped?.length > 0 && (
                  <span className="text-amber-600 ml-1">· {r.clean_report.columns_dropped.length} cols dropped</span>
                )}
              </span>
            </div>
            <ChevronDown size={13} className={`text-ink-faint transition-transform ${reportOpen ? 'rotate-180' : ''}`} strokeWidth={2} />
          </button>

          {reportOpen && (
            <div className="border-t border-border max-h-[300px] overflow-y-auto">
              {r.clean_report.columns_dropped?.length > 0 && (
                <div className="px-3 py-2 bg-amber-50/50 border-b border-amber-200">
                  <p className="text-[10px] font-semibold text-amber-700 mb-1">Dropped Columns</p>
                  <div className="flex flex-wrap gap-1">
                    {r.clean_report.columns_dropped.map(c => (
                      <span key={c} className="px-1.5 py-0.5 rounded text-[9px] font-mono bg-amber-100 text-amber-800 border border-amber-200">{c}</span>
                    ))}
                  </div>
                </div>
              )}

              <table className="w-full text-[10px]">
                <thead>
                  <tr className="border-b border-border bg-surface">
                    <th className="text-left px-3 py-1.5 font-semibold text-ink-muted uppercase tracking-wider">Operation</th>
                    <th className="text-left px-3 py-1.5 font-semibold text-ink-muted uppercase tracking-wider">Column</th>
                    <th className="text-right px-3 py-1.5 font-semibold text-ink-muted uppercase tracking-wider">Cells</th>
                    <th className="text-left px-3 py-1.5 font-semibold text-ink-muted uppercase tracking-wider">Before → After</th>
                  </tr>
                </thead>
                <tbody>
                  {shownOps.map((op, i) => (
                    <tr key={i} className={`border-b border-border/50 ${op.operation === 'drop_null' ? 'bg-amber-50/30' : ''}`}>
                      <td className="px-3 py-1.5 font-mono text-ink">
                        <span className={`inline-block px-1.5 py-0.5 rounded text-[9px] font-semibold ${
                          op.operation === 'drop_null' ? 'bg-amber-100 text-amber-700 border border-amber-200'
                          : op.operation === 'fill_null' ? 'bg-emerald-50 text-emerald-700 border border-emerald-200'
                          : 'bg-blue-50 text-blue-700 border border-blue-200'
                        }`}>
                          {op.operation}
                        </span>
                      </td>
                      <td className="px-3 py-1.5 font-mono text-ink-muted">{op.column || '—'}</td>
                      <td className="px-3 py-1.5 font-mono text-ink text-right font-semibold">{op.cells_affected?.toLocaleString()}</td>
                      <td className="px-3 py-1.5">
                        {op.sample_before?.length > 0 ? (
                          <div className="space-y-0.5">
                            {op.sample_before.slice(0, 3).map((bv, j) => (
                              <div key={j} className="flex items-center gap-1 text-[9px] font-mono">
                                <span className="text-red-500 line-through truncate max-w-[80px]" title={bv}>{bv || '(empty)'}</span>
                                <span className="text-ink-faint">→</span>
                                <span className="text-emerald-600 truncate max-w-[80px]" title={op.sample_after?.[j]}>{op.sample_after?.[j] || '(empty)'}</span>
                              </div>
                            ))}
                            {op.sample_before.length > 3 && (
                              <span className="text-[8px] text-ink-faint">+{op.sample_before.length - 3} more…</span>
                            )}
                          </div>
                        ) : (
                          <span className="text-ink-faint">—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {visibleOps < allOps.length && (
                <div className="px-3 py-2 text-center border-t border-border">
                  <button
                    onClick={() => setVisibleOps(c => Math.min(c + OPS_PAGE, allOps.length))}
                    className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-surface hover:bg-border/60 border border-border text-ink-muted text-[10px] font-semibold transition-colors cursor-pointer"
                  >
                    Load More ({allOps.length - visibleOps} remaining)
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* ── Detailed Normalize Report ── */}
      {r?.normalize_report && (
        <NormalizeReportCard report={r.normalize_report} />
      )}

      {/* Dedup metadata: strategy, dry-run, match columns */}
      {showDuplicates && p && (
        <div className="space-y-2">
          <div className="flex flex-wrap items-center gap-1.5">
            <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-semibold border ${
              p.strategy === 'fuzzy'
                ? 'bg-amber-50 text-amber-700 border-amber-200'
                : 'bg-blue-50 text-blue-700 border-blue-200'
            }`}>
              {p.strategy === 'fuzzy' ? `Fuzzy (${p.fuzzy_threshold ?? 0.85})` : 'Exact match'}
            </span>
            {p.keep_strategy && (
              <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-semibold bg-surface-raised text-ink-muted border border-border">
                Keep: {p.keep_strategy}
              </span>
            )}
            {p.dry_run && (
              <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-semibold bg-violet-50 text-violet-700 border border-violet-200">
                Dry run
              </span>
            )}
          </div>
          {p.match_columns?.length > 0 && (
            <div>
              <p className="text-[10px] font-semibold text-ink-muted uppercase tracking-wider mb-1">Match columns ({p.match_columns.length})</p>
              <div className="flex flex-wrap gap-1">
                {p.match_columns.map(col => (
                  <span key={col} className="px-1.5 py-0.5 rounded text-[9px] font-mono bg-accent/8 text-accent border border-accent/20">
                    {col}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Duration (show separately when duplicates mode) */}
      {showDuplicates && r?.duration && (
        <p className="text-[11px] font-mono text-ink-faint">Duration: {r.duration}</p>
      )}

      {/* ── Detailed Dedup Report ── */}
      {r?.dedup_report && r.dedup_report.total_groups > 0 && (
        <DedupReportCard report={r.dedup_report} />
      )}

      {/* Quality after this step */}
      {r?.analysis?.overall_quality != null && (
        <div>
          <div className="flex items-center justify-between mb-1">
            <span className="text-[10px] font-semibold text-ink-muted uppercase tracking-wider">Data quality after {label}</span>
            <span className="text-[10px] font-mono text-ink-faint">
              {r.analysis.total_columns} cols · {r.analysis.total_rows} rows
            </span>
          </div>
          <QualityDelta before={prevQuality} after={r.analysis.overall_quality} />
        </div>
      )}

      {/* Column stats table */}
      <ColStatsTable columnStats={r?.column_stats} />

      {/* Actions */}
      <div className="flex items-center gap-3 flex-wrap pt-1">
        <DownloadBtn datasetId={r?.dataset_id} dryRun={r?.dry_run} />
        {/* Dry-run: download the duplicates CSV + preview button */}
        {showDuplicates && r?.duplicates_dataset_id && (
          <a
            href={`${BACKEND}/api/v1/datasets/${r.duplicates_dataset_id}/export`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-violet-50 hover:bg-violet-100 border border-violet-200 text-violet-700 text-[11px] font-semibold transition-colors cursor-pointer shadow-sm"
          >
            <FileDown size={12} strokeWidth={2.5} />
            Download Duplicates CSV
          </a>
        )}
        {showDuplicates && r?.duplicate_rows?.length > 0 && (
          <button
            onClick={() => setDupePreviewOpen(true)}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-surface-raised hover:bg-border border border-border text-ink text-[11px] font-semibold transition-colors cursor-pointer shadow-sm"
          >
            <Eye size={12} strokeWidth={2.5} />
            Preview Duplicates
          </button>
        )}
        {onProceed && (
          <button
            onClick={onProceed}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-accent hover:bg-accent-hover text-white text-xs font-semibold transition-colors cursor-pointer shadow-sm"
          >
            {nextLabel ?? 'Continue'}
            <ChevronRight size={13} strokeWidth={2.5} />
          </button>
        )}
      </div>

      {/* Duplicates Preview Dialog */}
      {showDuplicates && (
        <DuplicatesPreviewDialog
          open={dupePreviewOpen}
          onClose={() => setDupePreviewOpen(false)}
          rows={r?.duplicate_rows ?? []}
          columns={allColumns}
          totalDuplicates={r?.duplicates_found ?? 0}
          duplicatesDatasetId={r?.duplicates_dataset_id}
        />
      )}
    </div>
  )
}

// ──────────────────────────────────────────────────
// Stepper Panel — orchestrates right-side ETL flow
// ──────────────────────────────────────────────────

function StepperPanel({ uploading, uploadResult, uploadErr, pipeline, onOpenOptions }) {
  const { steps, viewStep, setViewStep, normHasRules, normApplicable, normInfo, initImport, previewClean, runClean, previewNormalize, runNormalize, runDedup, reset } = pipeline

  // Compute quality at each stage for delta display
  const importQuality = uploadResult?.analysis?.overall_quality ?? 0
  const cleanQuality  = steps.clean?.result?.analysis?.overall_quality ?? importQuality
  const normQuality   = steps.normalize?.result?.analysis?.overall_quality ?? cleanQuality

  // ── Temporary tooltip after import completes ──
  const [showAdvancedTip, setShowAdvancedTip] = useState(false)
  const tipShownRef = useRef(false)

  // Auto-init import step when uploadResult first arrives
  const prevResultRef = useRef(null)
  useEffect(() => {
    if (uploadResult && uploadResult !== prevResultRef.current) {
      prevResultRef.current = uploadResult
      initImport(uploadResult)
      // Show tooltip once after first import
      if (!tipShownRef.current) {
        tipShownRef.current = true
        setShowAdvancedTip(true)
        setTimeout(() => setShowAdvancedTip(false), 3000)
      }
    }
  }, [uploadResult, initImport])

  // ── Empty state ──
  if (!uploadResult && !uploadErr && !uploading) {
    return (
      <div className="rounded-xl border-2 border-dashed border-border bg-surface/60 overflow-hidden shadow-sm">
        <div className="flex flex-col items-center justify-center gap-3 py-14 px-6 text-center">
          <div className="w-10 h-10 rounded-full bg-surface-raised border border-border flex items-center justify-center">
            <Upload size={18} className="text-ink-faint" strokeWidth={1.75} />
          </div>
          <p className="text-sm font-medium text-ink-muted">No file selected</p>
          <p className="text-xs text-ink-faint max-w-xs">
            Pick a <span className="font-mono">.csv</span> file using <em>Start Import</em> — then walk through Clean → Normalize → Dedup
          </p>
        </div>
      </div>
    )
  }

  // ── Uploading ──
  if (uploading) {
    return (
      <div className="rounded-xl border border-border bg-surface overflow-hidden shadow-sm">
        <div className="flex flex-col items-center justify-center gap-3 py-14 px-6">
          <Loader2 size={24} className="text-accent animate-spin" strokeWidth={2} />
          <p className="text-sm font-medium text-ink-muted">Uploading &amp; analyzing…</p>
          <p className="text-xs text-ink-faint">Parsing CSV &amp; running column analysis</p>
        </div>
      </div>
    )
  }

  // ── Upload error ──
  if (uploadErr) {
    return (
      <div className="rounded-xl border border-failed bg-failed-bg overflow-hidden shadow-sm">
        <div className="flex items-start gap-3 p-5">
          <AlertCircle size={16} className="text-failed shrink-0 mt-0.5" strokeWidth={2} />
          <div>
            <p className="text-sm font-semibold text-failed">Upload failed</p>
            <p className="text-xs text-ink-muted mt-1">{uploadErr}</p>
          </div>
        </div>
      </div>
    )
  }

  // ── Main pipeline stepper ──
  return (
    <div className="rounded-xl border border-border bg-surface overflow-hidden shadow-sm">
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-3 border-b border-border bg-surface-raised">
        <span className="text-xs font-semibold text-ink">ETL Pipeline</span>
        <div className="flex items-center gap-3">
          {onOpenOptions && (
            <div className="relative">
              <button onClick={onOpenOptions} className="text-[11px] text-ink-faint hover:text-ink transition-colors cursor-pointer flex items-center gap-1">
                <SlidersHorizontal size={11} strokeWidth={2.5} />
                Advanced Options
              </button>
              {/* Temporary tooltip after import */}
              {showAdvancedTip && (
                <div className="absolute top-full right-0 mt-2 z-50 animate-in fade-in slide-in-from-top-1 duration-300">
                  <div className="relative bg-accent text-white text-[10px] font-medium px-3 py-1.5 rounded-lg shadow-lg whitespace-nowrap">
                    {/* Arrow pointing up */}
                    <div className="absolute -top-1.5 right-4 w-3 h-3 bg-accent rotate-45 rounded-sm" />
                    <span className="relative z-10">Modify advanced options here</span>
                  </div>
                </div>
              )}
            </div>
          )}
          <button onClick={reset} className="text-[11px] text-ink-faint hover:text-ink transition-colors cursor-pointer flex items-center gap-1">
            <X size={11} strokeWidth={2.5} />
            Reset
          </button>
        </div>
      </div>

      {/* Step tabs */}
      <StepIndicator steps={steps} viewStep={viewStep} setViewStep={setViewStep} />

      {/* Step content */}
      <div className="max-h-[620px] overflow-y-auto">
        {viewStep === 0 && steps.import.status === 'done' && (
          <ImportStepCard
            uploadResult={uploadResult}
            onProceed={() => {
              if (steps.clean.status === 'idle') previewClean(uploadResult)
              else setViewStep(1)
            }}
          />
        )}
        {viewStep === 1 && (
          <ETLStepCard
            step={steps.clean}
            label="Clean"
            prevQuality={importQuality}
            nextLabel={normHasRules ? 'Proceed to Normalize' : 'Proceed to Dedup'}
            onRun={() => runClean(uploadResult, steps.clean.cleanRules)}
            onProceed={() => {
              if (normHasRules) {
                if (steps.normalize.status === 'idle') previewNormalize()
                else setViewStep(2)
              } else {
                if (steps.dedup.status === 'idle') runDedup(uploadResult)
                else setViewStep(3)
              }
            }}
          />
        )}
        {viewStep === 2 && (
          <ETLStepCard
            step={steps.normalize}
            label="Normalize"
            prevQuality={cleanQuality}
            nextLabel="Proceed to Dedup"
            normInfo={normInfo}
            onRun={(dbConfig) => runNormalize(uploadResult, steps.normalize.normRules, dbConfig)}
            onProceed={() => {
              if (steps.dedup.status === 'idle') runDedup(uploadResult)
              else setViewStep(3)
            }}
          />
        )}
        {viewStep === 3 && (
          <ETLStepCard
            step={steps.dedup}
            label="Dedup"
            showDuplicates
            allColumns={uploadResult?.columns}
            prevQuality={normQuality}
            onRun={() => runDedup(uploadResult)}
          />
        )}
      </div>
    </div>
  )
}

// ──────────────────────────────────────────────────
// App
// ──────────────────────────────────────────────────

export default function App() {
  const csv      = useCSVPicker()
  const etl      = useETLOptions()
  const pipeline = usePipeline(etl.opts)
  const [uploading, setUploading]       = useState(false)
  const [uploadResult, setResult]       = useState(null)
  const [uploadErr,  setUploadErr]      = useState('')
  const [etlDialogOpen, setEtlDialogOpen] = useState(false)

  // Auto-upload whenever a new validated file is picked
  useEffect(() => {
    if (!csv.file) return
    const ctrl = new AbortController()

    setUploading(true)
    setResult(null)
    setUploadErr('')
    pipeline.reset()

    const form = new FormData()
    form.append('file', csv.file)

    fetch(`${BACKEND}/api/v1/upload/csv`, {
      method: 'POST',
      body: form,
      signal: ctrl.signal,
    })
      .then(r => r.json())
      .then(data => {
        if (!data.success) throw new Error(data.error?.message ?? 'Upload failed')
        setResult(data.data)
      })
      .catch(err => {
        if (err.name !== 'AbortError') setUploadErr(err.message)
      })
      .finally(() => setUploading(false))

    return () => ctrl.abort()
  }, [csv.file]) // eslint-disable-line react-hooks/exhaustive-deps

  const handleClear = () => {
    csv.clear()
    setResult(null)
    setUploadErr('')
    pipeline.reset()
  }

  return (
    <div className="min-h-dvh bg-canvas text-ink">

      {/* Hidden file input */}
      <input
        ref={csv.inputRef}
        type="file"
        accept=".csv"
        className="hidden"
        onChange={csv.onChange}
      />

      {/* ETL Options Dialog */}
      <ETLOptionsDialog
        open={etlDialogOpen}
        onClose={() => setEtlDialogOpen(false)}
        opts={etl.opts}
        patch={etl.patch}
        columns={uploadResult?.columns ?? []}
        uploadResult={uploadResult}
      />

      <section className="dot-grid relative">
        <div className="absolute inset-x-0 bottom-0 h-px bg-border" />
        <div className="relative mx-auto max-w-7xl px-6 pt-20 pb-20">
          <div className="grid grid-cols-1 lg:grid-cols-[1fr_560px] gap-x-10 gap-y-12 items-start">

            {/* ── LEFT — brand copy + CTAs ── */}
            <div>
              <div className="flex items-center gap-3 mb-10">
                <Pill>
                  <StatusDot />
                  API&nbsp;Online
                </Pill>
                <span className="text-ink-faint text-xs font-mono">
                  Go 1.25 &middot; Learning project by Madhav
                </span>
              </div>

              <h1
                style={{ fontFamily: 'var(--font-display)' }}
                className="text-[clamp(3rem,6vw,5rem)] font-bold leading-none tracking-tight text-ink mb-4"
              >
                Data<span className="text-accent">Forge</span>
              </h1>

              <p
                style={{ fontFamily: 'var(--font-display)' }}
                className="text-xl font-medium text-ink-muted max-w-xl mb-4 leading-snug"
              >
                Distributed ETL pipeline engine.
                <br />
                Import, clean, normalize and deduplicate&nbsp;
                <span className="text-ink font-semibold">at scale</span>.
              </p>

              <p className="text-sm text-ink-faint max-w-lg leading-relaxed mb-10">
                Submit CSV import jobs to a concurrent worker pool, run multi-stage
                transformations, and expose clean datasets via a typed REST API — all
                without leaving the backend.
              </p>

              {/* CTA buttons */}
              <div className="flex items-center gap-3 flex-wrap mb-4">
                <button
                  onClick={csv.open}
                  disabled={uploading}
                  className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-accent hover:bg-accent-hover disabled:opacity-60 text-white text-sm font-semibold transition-colors duration-150 cursor-pointer shadow-sm"
                >
                  <Upload size={15} strokeWidth={2.5} />
                  Start Import
                </button>
                <button
                  onClick={() => document.getElementById('feedback')?.scrollIntoView({ behavior: 'smooth' })}
                  className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-surface hover:bg-surface-raised text-ink text-sm font-semibold border border-border hover:border-border-strong transition-colors duration-150 cursor-pointer shadow-sm"
                >
                  <MessageSquare size={15} strokeWidth={2.5} />
                  Raise an Issue
                </button>
              </div>

              {/* File chip — spinner while uploading, × when done */}
              {(csv.file || csv.error) && (
                <div className={`inline-flex items-center gap-2 mt-2 px-4 py-2.5 rounded-lg text-xs font-medium border ${
                  csv.error
                    ? 'bg-failed-bg border-failed text-failed'
                    : 'bg-done-bg border-done text-done'
                }`}>
                  {csv.error ? (
                    <><AlertCircle size={13} strokeWidth={2} />{csv.error}</>
                  ) : (
                    <>
                      <FileText size={13} strokeWidth={2} />
                      <span className="font-mono">{csv.file?.name}</span>
                      <span className="opacity-70">({(csv.file?.size / 1024).toFixed(0)} KB)</span>
                      {uploading ? (
                        <Loader2 size={12} strokeWidth={2.5} className="ml-1 animate-spin" />
                      ) : (
                        <button onClick={handleClear} className="ml-1 opacity-60 hover:opacity-100 transition-opacity cursor-pointer">
                          <X size={12} strokeWidth={2.5} />
                        </button>
                      )}
                    </>
                  )}
                </div>
              )}
            </div>

            {/* ── RIGHT — ETL stepper panel ── */}
            <div className="lg:sticky lg:top-10">
              <StepperPanel
                uploading={uploading}
                uploadResult={uploadResult}
                uploadErr={uploadErr}
                pipeline={pipeline}
                onOpenOptions={() => setEtlDialogOpen(true)}
              />
            </div>

          </div>
        </div>
      </section>

      <ContactForm />
      <AboutSection />

    </div>
  )
}
