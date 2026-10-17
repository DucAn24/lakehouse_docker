import { useState, useEffect } from 'react'
import { catalogAPI } from '../api'

export default function SampleData({ layer, table }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const load = () => {
    setLoading(true)
    setError(null)
    catalogAPI.tableSample(layer, table)
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }

  if (!data && !loading && !error) {
    return (
      <button className="btn" onClick={load}>
        Load sample data
      </button>
    )
  }

  if (loading) {
    return (
      <div className="loading-container">
        <div className="spinner" />
        <span>Loading sample…</span>
      </div>
    )
  }

  if (error) {
    return <p style={{ color: 'var(--accent-red)', fontSize: '0.85rem' }}>{error}</p>
  }

  if (!data?.data?.length) {
    return <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>No data available</p>
  }

  return (
    <div className="sample-table-wrapper">
      <table className="sample-table">
        <thead>
          <tr>
            {data.columns.map(col => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.data.map((row, i) => (
            <tr key={i}>
              {data.columns.map(col => (
                <td key={col} title={String(row[col] ?? '')}>
                  {row[col] === null ? <span style={{ opacity: 0.3 }}>null</span> : String(row[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
