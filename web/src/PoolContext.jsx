import { createContext, useContext, useEffect, useState } from 'react'
import { getNodes } from './api.js'

const PoolContext = createContext(null)
function usePoolPolling(enabled) {
  const [nodes, setNodes] = useState([])
  const [error, setError] = useState(null)
  useEffect(() => {
    if (!enabled) return
    let alive = true, timer
    async function poll() {
      try {
        const ns = await getNodes()
        if (alive) { setNodes([...ns].sort((a, b) => a.name.localeCompare(b.name))); setError(null) }
      } catch (e) { if (alive) setError(e.message) }
      if (alive) timer = setTimeout(poll, 3000)
    }
    poll()
    return () => { alive = false; clearTimeout(timer) }
  }, [enabled])
  return { nodes, setNodes, error }
}
export function PoolProvider({ children }) {
  const pool = usePoolPolling(true)
  return <PoolContext.Provider value={pool}>{children}</PoolContext.Provider>
}
export function usePool() {
  const shared = useContext(PoolContext)
  const local = usePoolPolling(!shared)
  return shared || local
}
