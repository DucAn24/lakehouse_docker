import { useRef, useCallback, useMemo, useState, useEffect } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  Position,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

// Custom node component
function LineageNodeComponent({ data }) {
  return (
    <div className={`lineage-node ${data.layer} ${data.highlight ? 'highlight' : ''}`}>
      <div className="lineage-node-label">{data.label}</div>
      <div className="lineage-node-layer">{data.layer}</div>
    </div>
  )
}

const nodeTypes = { lineageNode: LineageNodeComponent }

// Layout: position nodes in columns by layer
const LAYER_ORDER = ['source', 'bronze', 'silver', 'gold']
const LAYER_X = { source: 0, bronze: 300, silver: 600, gold: 900 }
const NODE_HEIGHT = 60
const NODE_GAP = 12

function layoutNodes(rawNodes) {
  const byLayer = {}
  for (const n of rawNodes) {
    const layer = n.layer || 'source'
    if (!byLayer[layer]) byLayer[layer] = []
    byLayer[layer].push(n)
  }

  const positioned = []
  for (const layer of LAYER_ORDER) {
    const items = byLayer[layer] || []
    const totalH = items.length * (NODE_HEIGHT + NODE_GAP)
    const startY = -totalH / 2

    items.forEach((n, i) => {
      positioned.push({
        id: n.id,
        type: 'lineageNode',
        position: { x: LAYER_X[layer] ?? 0, y: startY + i * (NODE_HEIGHT + NODE_GAP) },
        data: { label: n.label, layer: n.layer, highlight: n.highlight || false, table: n.table },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      })
    })
  }
  return positioned
}

function layoutEdges(rawEdges) {
  return rawEdges.map((e, i) => ({
    id: `e-${i}`,
    source: e.source,
    target: e.target,
    animated: true,
    style: { stroke: 'rgba(0, 212, 255, 0.3)', strokeWidth: 1.5 },
    markerEnd: { type: MarkerType.ArrowClosed, color: 'rgba(0, 212, 255, 0.4)', width: 14, height: 14 },
  }))
}

export default function LineageGraph({ graph, onNodeClick }) {
  const initialNodes = useMemo(() => layoutNodes(graph?.nodes || []), [graph])
  const initialEdges = useMemo(() => layoutEdges(graph?.edges || []), [graph])

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)

  useEffect(() => {
    setNodes(initialNodes)
    setEdges(initialEdges)
  }, [initialNodes, initialEdges, setNodes, setEdges])

  const handleNodeClick = useCallback((_event, node) => {
    if (onNodeClick) onNodeClick(node.data)
  }, [onNodeClick])

  if (!graph || !graph.nodes?.length) {
    return (
      <div className="empty-state">
        <h3>No lineage data</h3>
        <p>The lineage graph will appear once the pipeline has run.</p>
      </div>
    )
  }

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onNodeClick={handleNodeClick}
      nodeTypes={nodeTypes}
      fitView
      fitViewOptions={{ padding: 0.15 }}
      minZoom={0.2}
      maxZoom={2}
      proOptions={{ hideAttribution: true }}
    >
      <Background color="rgba(148, 163, 184, 0.06)" gap={20} />
      <Controls
        style={{ background: 'var(--bg-card)', borderColor: 'var(--border-subtle)' }}
      />
      <MiniMap
        nodeColor={(n) => {
          const colors = { source: '#7c3aed', bronze: '#cd7f32', silver: '#94a3b8', gold: '#f59e0b' }
          return colors[n.data?.layer] || '#64748b'
        }}
        style={{ background: 'var(--bg-deep)', borderColor: 'var(--border-subtle)' }}
      />
    </ReactFlow>
  )
}
