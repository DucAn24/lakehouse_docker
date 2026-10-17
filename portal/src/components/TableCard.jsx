import { useNavigate } from 'react-router-dom'
import DQBadge from './DQBadge'

export default function TableCard({ table }) {
  const navigate = useNavigate()

  return (
    <div
      className="table-card glass-card animate-slide-up"
      onClick={() => navigate(`/catalog/${table.layer}/${table.name}`)}
    >
      <div className="table-card-header">
        <span className="table-card-name">{table.name}</span>
        <span className={`badge badge-${table.layer}`}>{table.layer}</span>
      </div>

      {table.description && (
        <div className="table-card-desc">{table.description}</div>
      )}

      <div className="table-card-meta">
        <span>📊 {table.column_count ?? '?'} cols</span>
        {table.row_count !== null && table.row_count !== undefined && (
          <span>📝 {Number(table.row_count).toLocaleString()} rows</span>
        )}
        {table.dq_score !== null && table.dq_score !== undefined && (
          <DQBadge score={table.dq_score} />
        )}
      </div>
    </div>
  )
}
