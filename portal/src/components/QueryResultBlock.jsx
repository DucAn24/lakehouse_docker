import { useState } from 'react'
import ChartRenderer from './ChartRenderer'

export default function QueryResultBlock({ result }) {
  const [showSql, setShowSql] = useState(true)
  const [copied, setCopied] = useState(false)

  const copySQL = () => {
    navigator.clipboard.writeText(result.sql).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  return (
    <div className="query-result-block">
      {/* SQL */}
      <div className="query-result-header">
        <span onClick={() => setShowSql(!showSql)} style={{ cursor: 'pointer' }}>
          {showSql ? '▾' : '▸'} SQL Query
          {result.execution_ms != null && (
            <span style={{ marginLeft: 8, fontWeight: 400, opacity: 0.6 }}>{result.execution_ms}ms</span>
          )}
        </span>
        <button className="copy-btn" onClick={copySQL}>
          {copied ? '✓ Copied' : '📋 Copy'}
        </button>
      </div>
      {showSql && (
        <div className="query-sql">{result.sql}</div>
      )}

      {/* Chart */}
      {result.chart && (
        <div className="query-chart-container">
          <ChartRenderer chart={result.chart} />
        </div>
      )}

      {/* Data table */}
      {result.rows && result.rows.length > 0 && (
        <div className="query-data-table">
          <table className="sample-table">
            <thead>
              <tr>
                {result.columns.map(col => <th key={col}>{col}</th>)}
              </tr>
            </thead>
            <tbody>
              {result.rows.slice(0, 50).map((row, i) => (
                <tr key={i}>
                  {result.columns.map(col => (
                    <td key={col} title={String(row[col] ?? '')}>
                      {row[col] === null ? <span style={{ opacity: 0.3 }}>null</span> : String(row[col])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {result.row_count > 50 && (
            <div style={{ padding: '8px 12px', fontSize: '0.75rem', color: 'var(--text-muted)', textAlign: 'center' }}>
              Showing 50 of {result.row_count} rows
            </div>
          )}
        </div>
      )}
    </div>
  )
}
