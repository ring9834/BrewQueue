const money = new Intl.NumberFormat('en-NZ', { style: 'currency', currency: 'NZD' })
const time = new Intl.DateTimeFormat('en-NZ', { hour: 'numeric', minute: '2-digit' })

export const formatMoney = (value: number) => money.format(value)
export const formatTime = (iso: string) => time.format(new Date(iso))

export function minutesAgo(iso: string, now = Date.now()) {
  const minutes = Math.max(0, Math.round((now - new Date(iso).getTime()) / 60_000))
  return minutes === 0 ? 'just now' : `${minutes} min ago`
}
