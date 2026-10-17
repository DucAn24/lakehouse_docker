import { useState, useEffect, useMemo } from 'react'
import { catalogAPI } from '../api'
import TableCard from '../components/TableCard'

const LAYERS = ['all', 'bronze', 'silver', 'gold']

export default function CatalogPage() {
  const [tables, setTables] = useState([])
  const [stats, setStats] = useState(null)
  const [loading, setLoading] = useState(true)
  const [layer, setLayer] = useState('all')
  const [search, setSearch] = useState('')

  useEffect(() => {
    Promise.all([
      catalogAPI.tables(),
      catalogAPI.stats(),
    ]).then(([tablesRes, statsRes]) => {
      setTables(tablesRes.tables || [])
      setStats(statsRes)
    }).catch(err => {
      console.error('Failed to load catalog:', err)
    }).finally(() => setLoading(false))
  }, [])

  const filtered = useMemo(() => {
    let result = tables
    if (layer !== 'all') {
      result = result.filter(t => t.layer === layer)
    }
    if (search.trim()) {
      const q = search.toLowerCase()
      result = result.filter(t =>
        t.name.toLowerCase().includes(q) ||
        (t.description || '').toLowerCase().includes(q)
      )
    }
    return result
  }, [tables, layer, search])

  return (
    <div>
      <div className="page-header">
        <h1><span className="gradient-text">Data Catalog</span></h1>
        <p>Browse and discover tables across all layers of the lakehouse</p>
      </div>

      {/* Stats */}
      {stats && (
        <div className="stats-bar">
          <div className="stat-card glass-card animate-fade-in">
            <div className="stat-card-label">Total Tables</div>
            <div className="stat-card-value">{stats.total_tables}</div>
          </div>
          {stats.layers && Object.entries(stats.layers).map(([l, count]) => (
            <div className="stat-card glass-card animate-fade-in" key={l}>
              <div className="stat-card-label">{l}</div>
              <div className="stat-card-value">{count}</div>
              <div className="stat-card-sub">tables</div>
            </div>
          ))}
          {stats.total_gold_rows > 0 && (
            <div className="stat-card glass-card animate-fade-in">
              <div className="stat-card-label">Gold Rows</div>
              <div className="stat-card-value">{Number(stats.total_gold_rows).toLocaleString()}</div>
            </div>
          )}
        </div>
      )}

      {/* Search */}
      <div className="search-bar">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <input
          type="text"
          placeholder="Search tables, columns, descriptions…"
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
      </div>

      {/* Layer filter */}
      <div className="layer-tabs">
        {LAYERS.map(l => (
          <button
            key={l}
            className={`layer-tab${layer === l ? ' active' : ''}`}
            onClick={() => setLayer(l)}
          >
            {l} {l !== 'all' && stats?.layers?.[l] != null ? `(${stats.layers[l]})` : ''}
          </button>
        ))}
      </div>

      {/* Table grid */}
      {loading ? (
        <div className="loading-container">
          <div className="spinner" />
          <span>Loading catalog…</span>
        </div>
      ) : filtered.length === 0 ? (
        <div className="empty-state">
          <h3>No tables found</h3>
          <p>{search ? 'Try a different search term' : 'No tables registered in this layer'}</p>
        </div>
      ) : (
        <div className="table-grid stagger">
          {filtered.map(t => (
            <TableCard key={`${t.layer}/${t.name}`} table={t} />
          ))}
        </div>
      )}
    </div>
  )
}
