import {useEffect, useRef} from 'react'
import * as echarts from 'echarts/core'
import {LineChart, BarChart} from 'echarts/charts'
import {GridComponent, TooltipComponent, AriaComponent} from 'echarts/components'
import {CanvasRenderer} from 'echarts/renderers'
import type {EChartsCoreOption} from 'echarts/core'
echarts.use([LineChart, BarChart, GridComponent, TooltipComponent, AriaComponent, CanvasRenderer])

export function LiveChart({option, label, height = 280}: {option: EChartsCoreOption; label: string; height?: number}) {
  const element = useRef<HTMLDivElement>(null)
  const chart = useRef<echarts.EChartsType | null>(null)
  const lastOption = useRef('')
  useEffect(() => {
    lastOption.current = ''
    const serialized = JSON.stringify(option)
    if (lastOption.current === serialized) return
    lastOption.current = serialized
    chart.current = echarts.init(element.current!, undefined, {renderer: 'canvas'})
    const resize = new ResizeObserver(() => chart.current?.resize())
    resize.observe(element.current!)
    return () => {resize.disconnect(); chart.current?.dispose(); chart.current = null}
  }, [])
  useEffect(() => {
    const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches
    chart.current?.setOption({...option, animation: !reduced, animationDuration: 450,
      animationDurationUpdate: 450, animationEasingUpdate: 'cubicOut', aria: {enabled: true}}, {replaceMerge: ['series']})
  }, [option])
  return <div ref={element} role="img" aria-label={label} style={{height, width: '100%'}} />
}
