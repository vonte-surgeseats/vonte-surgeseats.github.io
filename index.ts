import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const SUPABASE_URL     = Deno.env.get('SUPABASE_URL')!
const SUPABASE_KEY     = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
const TICKETMASTER_KEY = Deno.env.get('TICKETMASTER_API_KEY')!
const TM_BASE          = 'https://app.ticketmaster.com/discovery/v2'
const EXPO_PUSH_URL    = 'https://exp.host/--/api/v2/push/send'

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

type Tier = 'alerts' | 'watchlist' | 'trending'

// ─── Fetch with timeout ───────────────────────────────────────────────────────

async function fetchWithTimeout(url: string, timeoutMs = 8000): Promise<Response> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const res = await fetch(url, { signal: controller.signal })
    clearTimeout(timer)
    return res
  } catch (err) {
    clearTimeout(timer)
    throw err
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function extractMinPrice(event: any): number | null {
  const range = event.priceRanges?.find((r: any) => r.type === 'standard') ?? event.priceRanges?.[0]
  return range?.min != null ? Math.round(range.min * 100) : null
}

function extractMaxPrice(event: any): number | null {
  const range = event.priceRanges?.find((r: any) => r.type === 'standard') ?? event.priceRanges?.[0]
  return range?.max != null ? Math.round(range.max * 100) : null
}

function getBestImage(images: any[]): string {
  return images?.find((i: any) => i.ratio === '16_9' && i.width >= 640)?.url ?? images?.[0]?.url ?? ''
}

function mapCategory(segment: string): string {
  const map: Record<string, string> = {
    'Music': 'concert', 'Sports': 'sports',
    'Arts & Theatre': 'theater', 'Comedy': 'comedy',
  }
  return map[segment] ?? 'other'
}

// ─── Fetch trending events (includes price data in the list call) ─────────────

async function fetchTMTrending(size = 20): Promise<any[]> {
  try {
    // Include includeTBA=no and include price ranges in the search
    const params = new URLSearchParams({
      apikey:     TICKETMASTER_KEY,
      size:       String(size),
      sort:       'relevance,desc',
      includeTBA: 'no',
      includeTBD: 'no',
    })
    const res = await fetchWithTimeout(`${TM_BASE}/events.json?${params}`)
    if (!res.ok) {
      console.error('TM trending fetch failed:', res.status, await res.text())
      return []
    }
    const data = await res.json()
    return data._embedded?.events ?? []
  } catch (err) {
    console.error('fetchTMTrending error:', err)
    return []
  }
}

// ─── Get event IDs for non-trending tiers ─────────────────────────────────────

async function getEventIds(tier: Tier): Promise<string[]> {
  if (tier === 'alerts') {
    const { data } = await supabase.from('price_alerts').select('event_id').eq('is_active', true).is('triggered_at', null)
    return [...new Set((data ?? []).map((r: any) => r.event_id))]
  }
  if (tier === 'watchlist') {
    const { data } = await supabase.from('watchlist').select('event_id')
    return [...new Set((data ?? []).map((r: any) => r.event_id))]
  }
  return []
}

// ─── Upsert venue ─────────────────────────────────────────────────────────────

async function upsertVenue(tm: any): Promise<string | null> {
  const venue = tm._embedded?.venues?.[0]
  if (!venue?.name) return null

  const venueData = {
    name:      venue.name,
    city:      venue.city?.name ?? '',
    state:     venue.state?.stateCode ?? '',
    country:   venue.country?.name ?? '',
    address:   venue.address?.line1 ?? '',
    latitude:  parseFloat(venue.location?.latitude ?? '0'),
    longitude: parseFloat(venue.location?.longitude ?? '0'),
  }

  const { data: existing } = await supabase
    .from('venues').select('id')
    .eq('name', venueData.name).eq('city', venueData.city)
    .maybeSingle()

  if (existing?.id) return existing.id

  const { data: created, error } = await supabase
    .from('venues').insert(venueData).select('id').single()

  if (error) { console.error('venue insert error:', error.message); return null }
  return created?.id ?? null
}

// ─── Upsert event ─────────────────────────────────────────────────────────────

async function upsertEvent(tm: any): Promise<void> {
  const venueId    = await upsertVenue(tm)
  const segment    = tm.classifications?.[0]?.segment?.name ?? 'other'
  const attraction = tm._embedded?.attractions?.[0]

  const eventData: any = {
    ticketmaster_id: tm.id,
    name:            tm.name,
    artist_or_team:  attraction?.name ?? tm.name,
    category:        mapCategory(segment),
    event_date:      tm.dates?.start?.dateTime ?? `${tm.dates?.start?.localDate}T${tm.dates?.start?.localTime ?? '00:00:00'}`,
    image_url:       getBestImage(tm.images ?? []),
    is_active:       true,
    updated_at:      new Date().toISOString(),
  }
  if (venueId) eventData.venue_id = venueId

  const { data: existing } = await supabase
    .from('events').select('id').eq('ticketmaster_id', tm.id).maybeSingle()

  if (existing?.id) {
    const { error } = await supabase.from('events').update(eventData).eq('id', existing.id)
    if (error) console.error('event update error:', error.message)
  } else {
    const { error } = await supabase.from('events').insert(eventData)
    if (error) console.error('event insert error:', error.message)
  }
}

// ─── Platform ID ──────────────────────────────────────────────────────────────

let tmPlatformId: string | null = null

async function getPlatformId(): Promise<string | null> {
  if (tmPlatformId) return tmPlatformId
  const { data } = await supabase.from('platforms').select('id').eq('slug', 'ticketmaster').maybeSingle()
  tmPlatformId = data?.id ?? null
  return tmPlatformId
}

// ─── Write prices ─────────────────────────────────────────────────────────────

async function writePrices(tmEventId: string, platformId: string, minPrice: number, maxPrice: number | null) {
  const now = new Date().toISOString()

  const { error: le } = await supabase.from('ticket_listings').upsert(
    { event_id: tmEventId, platform_id: platformId, min_price: minPrice, max_price: maxPrice, updated_at: now },
    { onConflict: 'event_id,platform_id' }
  )
  if (le) console.error('ticket_listings upsert error:', le.message, le.details)

  const { error: he } = await supabase.from('price_history').insert(
    { event_id: tmEventId, platform_id: platformId, min_price: minPrice, max_price: maxPrice, recorded_at: now }
  )
  if (he) console.error('price_history insert error:', he.message, he.details)

  await supabase.from('api_sync_log').insert(
    { event_id: tmEventId, platform_id: platformId, synced_at: now, status: 'success' }
  )
}

// ─── Process single event ─────────────────────────────────────────────────────

async function processEvent(tm: any, platformId: string): Promise<boolean> {
  console.log(`processing: ${tm.name} (${tm.id})`)

  const minPrice = extractMinPrice(tm)
  const maxPrice = extractMaxPrice(tm)

  console.log(`  prices: min=${minPrice} max=${maxPrice}`)

  await upsertEvent(tm)
  console.log(`  event upserted`)

  if (minPrice != null) {
    await writePrices(tm.id, platformId, minPrice, maxPrice)
    console.log(`  prices written`)
  } else {
    console.log(`  no price data from TM for this event`)
  }

  return true
}

// ─── Main handler ─────────────────────────────────────────────────────────────

Deno.serve(async (req) => {
  const url  = new URL(req.url)
  const tier = (url.searchParams.get('tier') ?? 'watchlist') as Tier

  console.log(`[sync] Starting tier=${tier}`)

  try {
    const platformId = await getPlatformId()
    if (!platformId) {
      console.error('No Ticketmaster platform record found')
      return new Response(JSON.stringify({ error: 'No platform record' }), { status: 500 })
    }
    console.log(`platformId=${platformId}`)

    let tmEvents: any[] = []

    if (tier === 'trending') {
      // For trending we already have full event objects from the list API
      tmEvents = await fetchTMTrending(20) // reduced to 20 to avoid timeout
      console.log(`fetched ${tmEvents.length} trending events from TM`)
    } else {
      const eventIds = await getEventIds(tier)
      console.log(`found ${eventIds.length} event IDs for tier=${tier}`)
      // For watchlist/alerts we need to fetch each event individually
      // Process sequentially to avoid timeout
      let synced = 0, failed = 0, noprice = 0
      for (const eventId of eventIds.slice(0, 15)) { // cap at 15 to avoid timeout
        try {
          const res = await fetchWithTimeout(`${TM_BASE}/events/${eventId}.json?apikey=${TICKETMASTER_KEY}`)
          if (!res.ok) { failed++; continue }
          const tm = await res.json()
          await processEvent(tm, platformId)
          synced++
        } catch (err) {
          console.error(`failed ${eventId}:`, err)
          failed++
        }
        await new Promise(r => setTimeout(r, 200))
      }
      return new Response(JSON.stringify({ tier, synced, failed, total: eventIds.length }), {
        status: 200, headers: { 'Content-Type': 'application/json' }
      })
    }

    if (tmEvents.length === 0) {
      return new Response(JSON.stringify({ tier, synced: 0, message: 'No events from TM' }), {
        status: 200, headers: { 'Content-Type': 'application/json' }
      })
    }

    // Process trending events sequentially (already have data, no extra API calls)
    let synced = 0, failed = 0, noprice = 0

    for (const tm of tmEvents) {
      try {
        const ok = await processEvent(tm, platformId)
        if (ok) {
          const mp = extractMinPrice(tm)
          if (mp != null) synced++; else noprice++
        }
      } catch (err) {
        console.error(`failed ${tm.id}:`, err)
        failed++
      }
    }

    console.log(`[sync] Done synced=${synced} noprice=${noprice} failed=${failed}`)

    return new Response(JSON.stringify({ tier, synced, noprice, failed, total: tmEvents.length }), {
      status: 200, headers: { 'Content-Type': 'application/json' }
    })

  } catch (err) {
    console.error('[sync] Fatal:', err)
    return new Response(JSON.stringify({ error: String(err) }), {
      status: 500, headers: { 'Content-Type': 'application/json' }
    })
  }
})
