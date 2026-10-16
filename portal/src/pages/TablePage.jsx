import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { catalogAPI } from '../api'
import ColumnTable from '../components/ColumnTable'
import SampleData from '../components/SampleData'
import DQBadge from '../components/DQBadge'

export default function TablePage() {
  const { layer, table } = useParams()
  const navigate = useNavigate()
  const [info, setInfo] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    catalogAPI.tableDetail(layer, table)
      .then(setInfo)
      .catch(err => console.error(err))
      .finally(() => setLoading(false))
  }, [layer, table])

  if (loading) {
    return (
      <div className="loading-container">
        <div className="spinner" />
        <span>Loading table details…</span>
      </div>
    )
  }

  if (!info) {
    return (
      <div className="empty-state">
        <h3>Table not found</h3>
        <p>{layer}/{table}</p>
      </div>
    )
  }

  return (
    <div className="table-detail animate-fade-in">
      <button className="back-btn" onClick={() => navigate('/catalog')}>
        ← Back to Catalog
      </button>

      <div className="table-detail-header">
        <h1>{info.name}</h1>
        <span className={`badge badge-${info.layer}`}>{info.layer}</span>
        <DQBadge score={info.dq_score} />
      </div>

      {info.description && (
        <p style={{ color: 'var(--text-secondary)', marginTop: 4, marginBottom: 16 }}>
          {info.description}
        </p>
      )}

      <div className="detail-grid">
        {/* Main content: columns */}
        <div>
          <div className="detail-card glass-card">
            <h3>Columns ({info.columns?.length || 0})</h3>
            <ColumnTable columns={info.columns} />
          </div>

          {/* Sample data */}
          {(layer === 'gold' || layer === 'silver') && (
            <div className="detail-card glass-card" style={{ marginTop: 16 }}>
              <h3>Sample Data</h3>
              <SampleData layer={layer} table={table} />
            </div>
          )}
        </div>

        {/* Sidebar: metadata */}
        <div className="detail-sidebar">
          <div className="detail-card glass-card">
            <h3>Properties</h3>
            <div className="detail-stat">
              <span className="label">Layer</span>
              <span className="value" style={{ textTransform: 'capitalize' }}>{info.layer}</span>
            </div>
            <div className="detail-stat">
              <span className="label">Columns</span>
              <span className="value">{info.column_count ?? '—'}</span>
            </div>
            {info.row_count != null && (
              <div className="detail-stat">
                <span className="label">Row Count</span>
                <span className="value">{Number(info.row_count).toLocaleString()}</span>
              </div>
            )}
            <div className="detail-stat">
              <span className="label">DQ Score</span>
              <span className="value">
                <DQBadge score={info.dq_score} />
              </span>
            </div>
            {info.dq_last_checked && (
              <div className="detail-stat">
                <span className="label">Last DQ Check</span>
                <span className="value" style={{ fontSize: '0.8rem' }}>{info.dq_last_checked}</span>
              </div>
            )}
          </div>

          <div className="detail-card glass-card">
            <h3>Storage</h3>
            <div style={{ fontFamily: 'var(--font-mono)', fontSize: '0.78rem', color: 'var(--text-secondary)', wordBreak: 'break-all' }}>
              {info.path}
            </div>
          </div>

          {info.primary_keys?.length > 0 && (
            <div className="detail-card glass-card">
              <h3>Primary Keys</h3>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {info.primary_keys.map(k => (
                  <span key={k} className="col-test-badge">{k}</span>
                ))}
              </div>
            </div>
          )}

          {/* FK relationships */}
          {info.columns?.some(c => c.fk) && (
            <div className="detail-card glass-card">
              <h3>Relationships</h3>
              {info.columns.filter(c => c.fk).map(c => (
                <div key={c.column} className="detail-stat">
                  <span className="label" style={{ fontFamily: 'var(--font-mono)', fontSize: '0.78rem' }}>{c.column}</span>
                  <span className="value col-fk" style={{ fontSize: '0.78rem' }}>→ {c.fk}</span>
                </div>
              ))}
            </div>
          )}

          <button
            className="btn"
            style={{ width: '100%', justifyContent: 'center' }}
            onClick={() => navigate(`/lineage?focus=${table}`)}
          >
            View Lineage →
          </button>
        </div>
      </div>
    </div>
  )
}
