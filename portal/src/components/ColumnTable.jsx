export default function ColumnTable({ columns }) {
  if (!columns || columns.length === 0) {
    return <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>No column information available</p>
  }

  return (
    <div className="column-table-wrapper">
      <table className="column-table">
        <thead>
          <tr>
            <th>Column</th>
            <th>Type</th>
            <th>FK</th>
            <th>Description</th>
            <th>Tests</th>
          </tr>
        </thead>
        <tbody>
          {columns.map((col, i) => (
            <tr key={col.column || i}>
              <td><span className="col-name">{col.column}</span></td>
              <td><span className="col-type">{col.type}</span></td>
              <td>
                {col.fk && <span className="col-fk">→ {col.fk}</span>}
              </td>
              <td>
                {col.description && <span className="col-desc">{col.description}</span>}
              </td>
              <td>
                {col.tests && col.tests.length > 0 && (
                  <div className="col-tests">
                    {col.tests.map((t, j) => (
                      <span key={j} className="col-test-badge">{t}</span>
                    ))}
                  </div>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
