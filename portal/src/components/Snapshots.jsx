import { useState, useEffect } from 'react'
import { catalogAPI } from '../api'

const fmt = (n) => (n == null ? '—' : Number(n).toLocaleString())

export default function Snapshots({ layer, table }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    catalogAPI.tableSnapshots(layer, table)
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [layer, table])

  if (loading) {
    return (
      <div className="loading-container">
        <div className="spinner" />
        <span>Loading snapshots…</span>
      </div>
    )
  }

  if (error) {
    return <p style={{ color: 'var(--accent-red)', fontSize: '0.85rem' }}>{error}</p>
  }

  if (!data?.snapshots?.length) {
    return <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>No snapshots yet</p>
  }

  return (
    <div className="sample-table-wrapper">
      <table className="sample-table">
        <thead>
          <tr>
            <th>Committed</th>
            <th>Operation</th>
            <th>+ Records</th>
            <th>− Records</th>
            <th>Total Records</th>
            <th>Files</th>
            <th>Snapshot ID</th>
          </tr>
        </thead>
        <tbody>
          {data.snapshots.map(s => (
            <tr key={s.snapshot_id}>
              <td title={s.committed_at}>{s.committed_at?.replace('T', ' ').slice(0, 19)}</td>
              <td>
                {s.operation}
                {s.is_current && <span className="col-test-badge" style={{ marginLeft: 6 }}>current</span>}
              </td>
              <td>{fmt(s.added_records)}</td>
              <td>{fmt(s.deleted_records)}</td>
              <td>{fmt(s.total_records)}</td>
              <td>{fmt(s.total_files)}</td>
              <td style={{ fontFamily: 'var(--font-mono)', fontSize: '0.75rem' }}>{s.snapshot_id}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
