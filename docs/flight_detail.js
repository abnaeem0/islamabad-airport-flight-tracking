document.addEventListener('DOMContentLoaded', async () => {

  // ===== URL PARAM VALIDATION =====
  const urlParams    = new URLSearchParams(window.location.search);
  const flightNumber = urlParams.get('flight');
  const date         = urlParams.get('date');
  const typeParam    = urlParams.get('type');   // 'Arrival' | 'Departure' | null (old links)
  const flightInfoDiv = document.getElementById('flight-info');

  if (!flightNumber || !date) {
    flightInfoDiv.innerHTML = '<p>Invalid page link. No flight or date specified. <a href="index.html">Go back to search</a>.</p>';
    document.getElementById('snapshot-controls').style.display = 'none';
    document.getElementById('snapshot-table').style.display   = 'none';
    return;
  }

  // ===== SUPABASE =====
  if (typeof supabase === 'undefined') {
    flightInfoDiv.innerHTML = '<p>Flight history is unavailable right now because a required script did not load. Please reload the page.</p>';
    document.getElementById('snapshot-controls').style.display = 'none';
    document.getElementById('snapshot-table').style.display   = 'none';
    return;
  }
  const supabaseUrl = 'https://sbaweaytsmdmhaclgcwr.supabase.co';
  const supabaseKey = 'sb_publishable_PBY7Y_HM60Ijqw9j6iOGeg_XqLDI7SS';
  const client      = supabase.createClient(supabaseUrl, supabaseKey);

  // ===== AIRLINE LOOKUP =====
  // Airline list, flight-number splitting and FlightStats codes live in airlines.js (shared with
  // the search page).
  const { AIRLINE_INFO, splitFlightNumber, getAirlineInfo } = window.Airlines;

  // ===== DEPARTURE DATE AT THE ORIGIN (FlightStats link, BL-039) =====
  // FlightStats files a flight under the day it leaves its origin. Islamabad's board gives only
  // the arrival date and time, so for arrivals with no departure-board match we estimate:
  // Islamabad scheduled arrival minus the typical flight time, read as a date in the origin's
  // time zone. A wrong link costs trust, so if there is any doubt (the estimate lands within
  // NEAR_MIDNIGHT minutes of midnight at the origin, the city is unknown or the time is missing)
  // the "Check it yourself" row offers both days, our guess first (BL-054).
  //
  // Origin city (as Islamabad's board writes it; case, spaces and punctuation ignored):
  //   [time zone, typical scheduled flight minutes to Islamabad]
  const ORIGIN_TIMES = {
    // Pakistan (used only when the departure board gives no match)
    karachi: ['Asia/Karachi', 120], lahore: ['Asia/Karachi', 55], peshawar: ['Asia/Karachi', 40],
    faisalabad: ['Asia/Karachi', 45], multan: ['Asia/Karachi', 60], quetta: ['Asia/Karachi', 95],
    gilgit: ['Asia/Karachi', 60], skardu: ['Asia/Karachi', 60], sukkur: ['Asia/Karachi', 85],
    gwadar: ['Asia/Karachi', 135], turbat: ['Asia/Karachi', 130], bahawalpur: ['Asia/Karachi', 70],
    rahimyarkhan: ['Asia/Karachi', 80], dgkhan: ['Asia/Karachi', 70], deraghazikhan: ['Asia/Karachi', 70],
    sialkot: ['Asia/Karachi', 40], chitral: ['Asia/Karachi', 60],
    // Gulf
    dubai: ['Asia/Dubai', 180], sharjah: ['Asia/Dubai', 175], abudhabi: ['Asia/Dubai', 180],
    rasalkhaimah: ['Asia/Dubai', 170], alain: ['Asia/Dubai', 175], fujairah: ['Asia/Dubai', 165],
    muscat: ['Asia/Muscat', 165], doha: ['Asia/Qatar', 200], bahrain: ['Asia/Bahrain', 210],
    kuwait: ['Asia/Kuwait', 230],
    // Saudi Arabia
    dammam: ['Asia/Riyadh', 245], riyadh: ['Asia/Riyadh', 255], jeddah: ['Asia/Riyadh', 295],
    madinah: ['Asia/Riyadh', 280], almadinah: ['Asia/Riyadh', 280], medina: ['Asia/Riyadh', 280],
    madina: ['Asia/Riyadh', 280], qassim: ['Asia/Riyadh', 260], alqassim: ['Asia/Riyadh', 260],
    gassim: ['Asia/Riyadh', 260], taif: ['Asia/Riyadh', 290], abha: ['Asia/Riyadh', 300],
    tabuk: ['Asia/Riyadh', 300], yanbu: ['Asia/Riyadh', 290], alula: ['Asia/Riyadh', 285],
    // Elsewhere
    istanbul: ['Europe/Istanbul', 330], london: ['Europe/London', 465], manchester: ['Europe/London', 455],
    birmingham: ['Europe/London', 460], paris: ['Europe/Paris', 420], toronto: ['America/Toronto', 855],
    baku: ['Asia/Baku', 185], tashkent: ['Asia/Tashkent', 105], kabul: ['Asia/Kabul', 65],
    najaf: ['Asia/Baghdad', 225], baghdad: ['Asia/Baghdad', 225], mashhad: ['Asia/Tehran', 140],
    tehran: ['Asia/Tehran', 180], urumqi: ['Asia/Shanghai', 160], kashgar: ['Asia/Shanghai', 90],
    beijing: ['Asia/Shanghai', 395], bangkok: ['Asia/Bangkok', 300], kualalumpur: ['Asia/Kuala_Lumpur', 360],
  };
  // CORRECTIONS: when a FlightStats link opens the wrong day, add the flight here with its real
  // scheduled flight time in minutes (FlightStats shows both times with their time zones).
  // Example: PK246 leaves Dammam 23:30 (+03), lands Islamabad 05:35 (+05) -> 4 h 05 = 245.
  const FLIGHT_MINUTES = {
    PK246: 245,
  };
  const NEAR_MIDNIGHT = 120;  // minutes: closer than this to midnight at the origin -> ask
  const NEAR_MIDNIGHT_CORRECTED = 10;

  function ymdIn(timeZone, ms) {
    const p = {};
    new Intl.DateTimeFormat('en-GB', { timeZone, year: 'numeric', month: '2-digit', day: '2-digit' })
      .formatToParts(new Date(ms)).forEach(x => { p[x.type] = x.value; });
    return `${p.year}-${p.month}-${p.day}`;
  }

  // Estimated departure day at the origin: { date: 'YYYY-MM-DD', sure: bool }, or null when the
  // city or the time is unknown. sure = the same date even if the flight time is off by the margin.
  function estimateDeparture(flightNum, city, arrDate, st) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(arrDate || '');
    const arrMin = toMinutes(st);
    const place = ORIGIN_TIMES[String(city || '').toLowerCase().replace(/[^a-z]/g, '')];
    if (!m || arrMin === null || !place) return null;
    const fixed  = FLIGHT_MINUTES[String(flightNum || '').toUpperCase()];
    const flight = fixed || place[1];
    const margin = fixed ? NEAR_MIDNIGHT_CORRECTED : NEAR_MIDNIGHT;
    const arrUtc = Date.UTC(+m[1], +m[2] - 1, +m[3]) + (arrMin - 5 * 60) * 60000;  // PKT is UTC+5
    const depAt  = extra => ymdIn(place[0], arrUtc - (flight + extra) * 60000);
    const day = depAt(0);
    return { date: day, sure: depAt(-margin) === day && depAt(margin) === day };
  }

  function dayBefore(iso) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || '');
    return m ? new Date(Date.UTC(+m[1], +m[2] - 1, +m[3] - 1)).toISOString().slice(0, 10) : null;
  }

  // "Sun 8 Nov"
  function dayLabel(iso) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || '');
    if (!m) return '';
    return new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]))
      .toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' })
      .replace(',', '');
  }

  // ===== UTILS =====

  // scraped_at is stored as a UTC clock value without a timezone marker, so a value
  // with no zone is treated as UTC and shown in Pakistan time.
  function formatPKT(dateStr) {
    const normalized = dateStr.endsWith('Z') || dateStr.includes('+') ? dateStr : dateStr + 'Z';
    const utcDate = new Date(normalized);
    return utcDate.toLocaleString('en-GB', {
      timeZone:   'Asia/Karachi',
      year:       'numeric',
      month:      '2-digit',
      day:        '2-digit',
      hour:       '2-digit',
      minute:     '2-digit',
    });
  }

  // Escape text before it is placed into innerHTML (values come from a third-party feed)
  function esc(val) {
    return String(val).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
                      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  // HTML-safe display text; missing values show a dash
  function display(val) {
    return val == null ? '—' : esc(val);
  }

  function timeAgo(dateStr) {
    const diffMs   = Date.now() - new Date(dateStr).getTime();
    const diffMins = Math.floor(diffMs / 60000);
    if (diffMins < 1)  return 'just now';
    if (diffMins < 60) return `${diffMins} min${diffMins > 1 ? 's' : ''} ago`;
    const diffHrs = Math.floor(diffMins / 60);
    return `${diffHrs} hour${diffHrs > 1 ? 's' : ''} ago`;
  }

  function changeLabel(changeType) {
    switch (changeType) {
      case 'new':           return '🆕 First seen';
      case 'status_change': return '🔄 Status changed';
      case 'time_change':   return '🕐 Time changed';
      case 'city_change':   return '📍 City changed';
      case 'dropped':       return '⚠️ Dropped';
      default:              return '';
    }
  }

  // "15:55" or "15:55:00" -> minutes since midnight; anything else -> null
  function toMinutes(t) {
    const m = /^(\d{1,2}):(\d{2})/.exec(String(t || '').trim());
    return m ? Number(m[1]) * 60 + Number(m[2]) : null;
  }

  // Expected minus scheduled, in minutes (wraps past midnight); null if either is unreadable
  function minutesDiff(st, et) {
    const a = toMinutes(st), b = toMinutes(et);
    if (a === null || b === null) return null;
    let d = b - a;
    if (d > 720)  d -= 1440;
    if (d < -720) d += 1440;
    return d;
  }

  function diffText(d) {
    if (d === null || d === 0) return '';
    const n = Math.abs(d);
    const txt = n >= 60 ? `${Math.floor(n / 60)} h ${n % 60 ? (n % 60) + ' min' : ''}`.trim() : `${n} min`;
    return d < 0 ? `${txt} early` : `${txt} late`;
  }

  // Colour group for a board status (display only; the text itself is always shown as-is)
  function statusTone(status) {
    const s = String(status || '').toLowerCase();
    if (!s)                                   return 'none';
    if (/cancel/.test(s))                     return 'bad';
    if (/drop/.test(s))                       return 'muted';
    if (/land|arriv/.test(s))                 return 'good';
    if (/delay|late/.test(s))                 return 'warn';
    if (/advanc|early/.test(s))               return 'info';
    if (/depart|airborne|take/.test(s))       return 'info';
    return 'neutral';
  }

  function statusPill(status) {
    return `<span class="status-pill status-pill--${statusTone(status)}">${display(status || 'No status')}</span>`;
  }

  // Islamabad's line: big time (expected if it differs, else scheduled), status, and the sub-line
  function legHtml({ label, st, et, status }) {
    const d       = minutesDiff(st, et);
    const hasEt   = et != null && String(et).trim() !== '' && String(et).trim() !== String(st || '').trim();
    const bigTime = hasEt ? et : st;
    const kind    = hasEt ? 'Expected' : 'Scheduled';
    const dtxt    = diffText(d);
    const sub = hasEt
      ? `Expected · scheduled ${display(st)}${dtxt ? ` · <strong class="delta delta--${d < 0 ? 'early' : 'late'}">${esc(dtxt)}</strong>` : ''}`
      : `${kind} · no expected time posted`;
    return `
      <div class="leg leg--primary">
        <p class="leg-label">${label}</p>
        <div class="leg-main">
          <span class="leg-time">${display(bigTime)}</span>
          ${statusPill(status)}
        </div>
        <p class="leg-sub">${sub}</p>
      </div>`;
  }

  function prettyDate(iso) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || '');
    if (!m) return display(iso);
    const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
    return esc(d.toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }));
  }

  // ===== STATE =====
  let lastScrapeTime = null;

  // ===== DOM REFS =====
  const lastRefreshedEl = document.getElementById('last-refreshed');
  const tbody           = document.querySelector('#snapshot-table tbody');

  // ===== SHARE MODAL =====

  // Inject modal HTML once into the page
  function injectShareModal() {
    if (document.getElementById('share-modal')) return;
    const modal = document.createElement('div');
    modal.id = 'share-modal';
    modal.innerHTML = `
      <div id="share-modal-backdrop"></div>
      <div id="share-modal-box" role="dialog" aria-modal="true" aria-labelledby="share-modal-title">
        <div id="share-modal-header">
          <h2 id="share-modal-title">Share Flight</h2>
          <button id="share-modal-close" aria-label="Close">&times;</button>
        </div>

        <label id="share-text-label" for="share-text">Message (you can edit it)</label>
        <textarea id="share-text" rows="7" spellcheck="false"></textarea>
        <label id="share-pnr-label" for="share-pnr">Booking reference (PNR), for your own reference <span>optional</span></label>
        <input id="share-pnr" type="text" maxlength="12" autocapitalize="characters" spellcheck="false" placeholder="e.g. 5XK2LM" autocomplete="off">

        <div id="share-actions">
          <div id="share-btn-row">
            <button id="share-copy-btn" class="share-btn share-btn--primary">
              <span id="share-copy-icon">📋</span> Copy
            </button>
            <button id="share-native-btn" class="share-btn share-btn--primary" hidden>
              <span>📤</span> Share
            </button>
          </div>

          <!-- WhatsApp -->
          <div id="share-wa-row">
            <label for="share-wa-input">Send on WhatsApp (to yourself or anyone)</label>
            <div id="share-wa-input-row">
              <span id="share-wa-prefix">+<input id="share-wa-code" type="tel" inputmode="numeric" maxlength="4" value="92" aria-label="Country code"></span>
              <input id="share-wa-input" type="tel" maxlength="15" placeholder="3001234567" inputmode="numeric" aria-label="WhatsApp number">
              <button id="share-wa-btn" class="share-btn share-btn--whatsapp" aria-label="Send on WhatsApp">
                <svg id="share-wa-icon" viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347z"/><path d="M12 0C5.373 0 0 5.373 0 12c0 2.124.558 4.122 1.532 5.862L.057 23.428a.75.75 0 0 0 .916.916l5.566-1.475A11.944 11.944 0 0 0 12 24c6.627 0 12-5.373 12-12S18.627 0 12 0zm0 21.75a9.694 9.694 0 0 1-4.953-1.357l-.355-.211-3.683.975.993-3.585-.232-.369A9.695 9.695 0 0 1 2.25 12C2.25 6.615 6.615 2.25 12 2.25S21.75 6.615 21.75 12 17.385 21.75 12 21.75z"/></svg>
                Send
              </button>
            </div>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(modal);

    // Close handlers
    document.getElementById('share-modal-close').addEventListener('click', closeShareModal);
    document.getElementById('share-modal-backdrop').addEventListener('click', closeShareModal);
    document.addEventListener('keydown', e => { if (e.key === 'Escape') closeShareModal(); });

    const messageText = () => document.getElementById('share-text').value;

    // Copy button
    document.getElementById('share-copy-btn').addEventListener('click', () => {
      navigator.clipboard.writeText(messageText()).then(() => {
        const btn = document.getElementById('share-copy-btn');
        btn.textContent = '✅ Copied!';
        setTimeout(() => { btn.innerHTML = '<span>📋</span> Copy'; }, 2000);
      });
    });

    // Booking reference: one line under the status line, swapped as it is typed (sender's edits kept)
    document.getElementById('share-pnr').addEventListener('input', e => setPnrLine(e.target.value));

    // Share button: the phone's own share sheet, shown only where the browser has one
    const nativeBtn = document.getElementById('share-native-btn');
    if (navigator.share) {
      nativeBtn.hidden = false;
      nativeBtn.addEventListener('click', () => {
        navigator.share({ text: messageText() }).catch(() => {});  // closing the sheet is not an error
      });
    }

    // WhatsApp button
    document.getElementById('share-wa-btn').addEventListener('click', () => {
      const code = document.getElementById('share-wa-code').value.replace(/\D/g, '');
      const raw  = document.getElementById('share-wa-input').value.trim().replace(/\D/g, '');
      if (!code) { document.getElementById('share-wa-code').focus(); return; }
      if (!raw)  { document.getElementById('share-wa-input').focus(); return; }
      const number = code + raw.replace(/^0+/, '');   // country code (default 92) + number without its leading 0
      window.open(`https://wa.me/${number}?text=${encodeURIComponent(messageText())}`, '_blank');
    });

  }

  let pnrLine = '';   // the booking-reference line currently in the message ('' = none)
  function setPnrLine(raw) {
    const ta  = document.getElementById('share-text');
    const v   = String(raw || '').replace(/\s+/g, '').toUpperCase();
    const nxt = v ? `Booking ref (PNR): ${v}` : '';
    let lines = ta.value.split('\n');
    if (pnrLine) lines = lines.filter(l => l !== pnrLine);
    if (nxt) {
      const i = lines.findIndex(l => l.startsWith('Airport status:'));
      lines.splice(i >= 0 ? i + 1 : 1, 0, nxt);
    }
    ta.value = lines.join('\n');
    pnrLine  = nxt;
  }

  // ----- Departure day (arrivals) -----
  // Which day the arrival left its origin (FlightStats files flights by that day):
  //   departure board (domestic, matched) > AeroDataBox origin leg (BL-040) > estimate.
  // source: 'board' | 'adb' | 'estimate'
  function departureDay(snapshots) {
    const latest = snapshots[snapshots.length - 1];
    if (dep && /^\d{4}-\d{2}-\d{2}$/.test(dep.row.departure_date || '')) {
      return { guess: dep.row.departure_date, sure: true, source: 'board' };
    }
    const od = adb && adb.origin_date;
    if (/^\d{4}-\d{2}-\d{2}$/.test(od || '') && (od === date || od === dayBefore(date))) {
      return { guess: od, sure: true, source: 'adb' };
    }
    const timed = [...snapshots].reverse().find(s => toMinutes(s.st) !== null) || latest;
    const est = estimateDeparture(flightNumber, latest.city || timed.city, date, timed.st);
    if (!est || (est.date !== date && est.date !== dayBefore(date))) return { guess: null, sure: false, source: 'estimate' };
    return { guess: est.date, sure: est.sure, source: 'estimate' };
  }

  function openShareModal(snapshots) {
    pnrLine = '';
    document.getElementById('share-pnr').value = '';
    document.getElementById('share-text').value = buildShareText(snapshots);
    document.getElementById('share-modal').classList.add('share-modal--open');
    document.body.style.overflow = 'hidden';
  }

  function closeShareModal() {
    document.getElementById('share-modal').classList.remove('share-modal--open');
    document.body.style.overflow = '';
  }

  // FlightStats link for a departure date ('YYYY-MM-DD'), or with no date when null.
  // Returns a maker that gives '' for every date when the flight number is not a normal one.
  function fsLinkMaker() {
    const { code, digits } = splitFlightNumber(flightNumber);
    if (!digits || (AIRLINE_INFO[code] && AIRLINE_INFO[code].fsCode === null)) return () => '';
    const fsCode = (AIRLINE_INFO[code] && AIRLINE_INFO[code].fsCode) || code;
    const base = `https://www.flightstats.com/v2/flight-tracker/${encodeURIComponent(fsCode)}/${parseInt(digits, 10)}`;
    return iso => {
      const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || '');
      return m ? `${base}?year=${m[1]}&month=${parseInt(m[2], 10)}&date=${parseInt(m[3], 10)}` : base;
    };
  }

  // Build the share message text from current page state
  // Share message (BL-054): one plain sentence that makes sense offline, the airport's status with
  // the time we last checked, our link, and the airline helpline. FlightStats is not in the message:
  // the page offers it ("Check it yourself"), and asks for the day there when we are not sure.
  function buildShareText(snapshots) {
    const latest    = snapshots[snapshots.length - 1];
    const airline   = getAirlineInfo(flightNumber);
    const named     = airline.name && airline.name !== 'Airline';
    const who       = `${named ? airline.name + ' flight' : 'Flight'} ${flightNumber}`;
    const city      = latest.city && latest.city !== '—' ? latest.city : '';
    const isArrival = (latest.type || '').toLowerCase() === 'arrival';
    const day       = dayLabel(date);
    const st = (latest.st || '').trim(), et = (latest.et || '').trim();
    const status = String(latest.status || '').trim();
    const tone   = statusTone(status);
    const d      = et && et !== st ? minutesDiff(st, et) : null;
    const asOf   = clockPKT(lastScrapeTime || latest.scraped_at);
    const verb   = isArrival ? 'land at' : 'leave';
    const leg    = isArrival ? `${who}${city ? ` from ${city}` : ''}` : `${who}${city ? ` to ${city}` : ''}`;
    const route  = isArrival ? `${who}${city ? ` from ${city}` : ''} to Islamabad` : `${who} from Islamabad${city ? ` to ${city}` : ''}`;

    let sentence;
    if (tone === 'bad') {
      sentence = `${route} on ${day} is cancelled.`;
    } else if (tone === 'muted') {
      sentence = `${route} on ${day} is no longer on the airport board. Please check with the airline.`;
    } else if (isArrival && tone === 'good') {
      sentence = `${leg} has landed at Islamabad airport (${day}).`;
    } else if (!isArrival && /depart|airborne|take/i.test(status)) {
      sentence = `${leg} has left Islamabad airport (${day}).`;
    } else if (d) {
      sentence = `${leg} is now expected to ${verb} Islamabad airport at ${et} on ${day}, ${diffText(d)} (scheduled ${st}).`;
    } else {
      sentence = `${leg} is due to ${verb} Islamabad airport at ${st || '—'} on ${day}.`;
    }
    const lines = [
      `✈️ ${sentence}`,
      `Airport status: ${status || 'none posted'}, as of ${asOf} PKT.`,
      '',
      `Live updates: ${window.location.href}`,
    ];
    if (airline.contact && airline.contact !== 'N/A') lines.push(`${airline.name} helpline: ${airline.contact}`);
    return lines.join('\n');
  }

  // ===== CHECK IT YOURSELF (BL-049) =====
  // The links the share message carries (FlightStats, the airline's status page, its helpline),
  // shown on the page as a row of chips under Islamabad's line. FlightStats uses the same day as
  // the share box: when that day is uncertain, both possible days get their own chip.
  function checkLinksHtml(snapshots) {
    const latest    = snapshots[snapshots.length - 1];
    const isArrival = (latest.type || '').toLowerCase() === 'arrival';
    const airline   = getAirlineInfo(flightNumber);
    const fsUrlFor  = fsLinkMaker();
    const chip = (href, text, ext, title) =>
      `<a class="check-chip" href="${esc(href)}"${title ? ` title="${esc(title)}"` : ''}${ext ? ' target="_blank" rel="noopener"' : ''}>${text}${ext ? ' <span class="check-ext" aria-hidden="true">↗</span>' : ''}</a>`;
    const chips = [];

    let credit = '';
    if (fsUrlFor(date)) {
      if (isArrival) {
        const d = departureDay(snapshots);
        if (d.sure) {
          chips.push(chip(fsUrlFor(d.guess), '🛰️ FlightStats', true));
          if (d.source === 'adb') credit = '<p class="check-credit">FlightStats day from <a href="https://aerodatabox.com/" target="_blank" rel="noopener">AeroDataBox</a></p>';
        } else {
          // Not sure which day it left: both days, our guess (if any) first and marked
          const days = d.guess === date ? [date, dayBefore(date)] : [dayBefore(date), date];
          days.forEach((iso, n) => chips.push(chip(fsUrlFor(iso),
            `${n === 0 ? '🛰️ FlightStats · ' : ''}left ${esc(dayLabel(iso))}${d.guess === iso ? ' (likely)' : ''}`, true)));
          const from = latest.city && latest.city !== '—' ? esc(latest.city) : 'its origin';
          credit = `<p class="check-credit">FlightStats lists flights by the day they leave ${from}.</p>`;
        }
      } else {
        chips.push(chip(fsUrlFor(date), '🛰️ FlightStats', true));
      }
    }
    if (airline.statusUrl && airline.statusUrl !== '#') {
      chips.push(chip(airline.statusUrl, `✈️ ${esc(airline.name)} status`, true));
    }
    // Helpline: dial the first number (the local one); the full text shows on hover
    if (airline.contact && airline.contact !== 'N/A') {
      const tel = String(airline.contact).split('(')[0].replace(/[^\d+]/g, '');
      if (tel) {
        chips.push(chip(`tel:${tel}`, `📞 Call ${esc(airline.name)}`, false, airline.contact));
      }
    }
    if (!chips.length) return '';
    return `
      <div class="check-links">
        <p class="check-label">Check it yourself</p>
        ${chips.join('')}
        ${credit}
      </div>`;
  }

  // Inject the Share button into #flight-info (called after header renders)
  function injectShareButton(snapshots) {
    const btn = document.createElement('button');
    btn.id        = 'share-btn';
    btn.className = 'share-trigger-btn';
    btn.innerHTML = '🔗 Share Flight';
    btn.addEventListener('click', () => openShareModal(snapshots));

    (flightInfoDiv.querySelector('.fi-head') || flightInfoDiv).appendChild(btn);
  }

  // ===== RENDER SNAPSHOTS =====
  // Table date: "6 Nov, 08:46" (Pakistan time), short enough to leave room for the status
  function shortPKT(dateStr) {
    const n = dateStr.endsWith('Z') || dateStr.includes('+') ? dateStr : dateStr + 'Z';
    const d = new Date(n);
    const day  = d.toLocaleDateString('en-GB', { timeZone: 'Asia/Karachi', day: 'numeric', month: 'short' });
    const time = d.toLocaleTimeString('en-GB', { timeZone: 'Asia/Karachi', hour: '2-digit', minute: '2-digit', hour12: false });
    return `${day}, ${time}`;
  }

  function renderSnapshots(snapshots) {
    tbody.innerHTML = '';

    if (!snapshots.length) {
      tbody.innerHTML = '<tr><td colspan="5">No changes recorded for this flight.</td></tr>';
      return;
    }

    snapshots.forEach(s => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${esc(shortPKT(s.scraped_at))}</td>
        <td>${display(s.st)}</td>
        <td>${display(s.et)}</td>
        <td>${display(s.status)}</td>
        <td>${changeLabel(s.change_type)}</td>
      `;
      tbody.appendChild(tr);
    });
  }

  // ===== DEPARTURE AIRPORT (domestic arrivals only, BL-038) =====
  // Islamabad's board is the main view. The departure airport's board (from isb_arrival_origin_view
  // and origin_snapshots) is used for two things only:
  //   1. a one-line notice when it shows something Islamabad's board has not caught up with;
  //   2. its own history, shown when the visitor picks it in the "Airport board" switch.
  // If the view is missing, unmatched or errors, neither appears and the page works as before.
  const noticeEl      = document.getElementById('board-notice');
  const historyTitle  = document.getElementById('history-title');
  const switchEl      = document.getElementById('board-switch');
  const boardMetaEl   = document.getElementById('board-meta');
  const isbCheckedEl  = document.getElementById('snapshot-controls');
  const STALE_MINUTES  = 60;  // departure-board data older than this is not used for the notice
  const NOTICE_MINUTES = 15;  // departure board this much later than Islamabad's board -> notice

  let board    = 'isb';  // which history is showing: 'isb' or 'dep'
  let isbSnaps = [];
  let dep      = null;   // { row, snaps } for a matched domestic arrival, else null
  let adb      = null;   // AeroDataBox origin leg for an arrival (adb_arrivals row), else null (BL-040)
  let isArrivalFlight = true;

  function minutesSince(dateStr) {
    return (Date.now() - new Date(dateStr).getTime()) / 60000;
  }

  function clockPKT(dateStr) {
    const n = dateStr.endsWith('Z') || dateStr.includes('+') ? dateStr : dateStr + 'Z';
    return new Date(n).toLocaleTimeString('en-GB', { timeZone: 'Asia/Karachi', hour: '2-digit', minute: '2-digit', hour12: false });
  }

  // AeroDataBox origin leg (BL-040): only the origin's day is used, for the FlightStats link.
  // Missing table, no row or any error -> null, and the estimate is used instead.
  async function fetchAdb(flightType) {
    if (flightType !== 'Arrival') return null;
    try {
      const { data, error } = await client
        .from('adb_arrivals')
        .select('origin_date,origin_iata,origin_sched_local')
        .eq('flight_number', flightNumber)
        .eq('isb_date', date)
        .limit(2);
      return (!error && Array.isArray(data) && data.length === 1) ? data[0] : null;   // two rows: ambiguous, skip
    } catch (e) {
      return null;
    }
  }

  async function fetchDeparture(flightType) {
    if (flightType !== 'Arrival') return null;
    try {
      const { data, error } = await client
        .from('isb_arrival_origin_view')
        .select('*')
        .eq('flight_number', flightNumber)
        .eq('arrival_date', date)
        .limit(1);
      if (error || !data || !data.length || data[0].match_state !== 'matched') return null;
      const row = data[0];
      const res = await client
        .from('origin_snapshots')
        .select('scraped_at,status,st,et,change_type')
        .eq('flight_number', row.flight_number)
        .eq('scheduled_date', row.departure_date)
        .eq('type', 'Departure')
        .eq('source_airport', row.departure_airport)
        .order('scraped_at', { ascending: true });
      return { row, snaps: (res.error || !res.data) ? [] : res.data };
    } catch (e) {
      return null;
    }
  }

  // The one-line notice, or null. Only when the departure board is fresh and shows something
  // Islamabad's board does not: cancelled, departed, or NOTICE_MINUTES+ later than Islamabad.
  function departureNotice(isb) {
    if (!dep) return null;
    const row = dep.row;
    if (!row.departure_checked || minutesSince(row.departure_checked) > STALE_MINUTES) return null;
    const name      = display(row.departure_airport);
    const depStatus = String(row.departure_status || '');
    const isbStatus = String(isb.status || '');
    const isbMoved  = /land|arriv|airborne|depart|en ?route|cancel/i.test(isbStatus);

    if (/cancel/i.test(depStatus) && !/cancel/i.test(isbStatus)) {
      return `${name}'s board shows this flight cancelled. Islamabad's board hasn't updated yet.`;
    }
    if (isbMoved) return null;
    if (/depart|airborne/i.test(depStatus)) {
      const seen = dep.snaps.find(s => /depart|airborne/i.test(s.status || ''));
      return `Departed ${name}${seen ? ` (seen ${esc(clockPKT(seen.scraped_at))})` : ''}. Islamabad's board hasn't updated yet.`;
    }
    const depLate = minutesDiff(row.departure_st, row.departure_et);
    if (depLate !== null && depLate > 0) {
      const isbLate = minutesDiff(isb.st, isb.et) || 0;
      if (depLate - isbLate >= NOTICE_MINUTES) {
        return `${name} shows ${esc(diffText(depLate))}. Islamabad shows ${isbLate ? esc(diffText(isbLate)) : 'on time'}.`;
      }
    }
    return null;
  }

  function renderNotice(isb) {
    const msg = departureNotice(isb);
    if (!msg) { noticeEl.hidden = true; noticeEl.innerHTML = ''; return; }
    noticeEl.innerHTML = `<p>${msg} <button type="button" class="notice-link">See ${display(dep.row.departure_airport)} board</button></p>`;
    noticeEl.hidden = false;
  }

  noticeEl.addEventListener('click', e => {
    if (!e.target.closest('.notice-link')) return;
    board = 'dep';
    renderBoard();
    historyTitle.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });

  switchEl.addEventListener('click', e => {
    const b = e.target.closest('button[data-board]');
    if (!b || b.dataset.board === board) return;
    board = b.dataset.board;
    renderBoard();
  });

  // History section: Islamabad's board, or the departure airport's when picked
  function renderBoard() {
    const hasDep = !!(dep && dep.snaps.length);
    if (!hasDep) board = 'isb';
    const isbName = `Islamabad ${isArrivalFlight ? 'arrivals' : 'departures'}`;

    if (!hasDep) {
      historyTitle.textContent = `${isbName} board history`;
      switchEl.hidden = true; switchEl.innerHTML = '';
      boardMetaEl.hidden = true; boardMetaEl.innerHTML = '';
      renderSnapshots(isbSnaps);
      return;
    }

    const depName = display(dep.row.departure_airport);
    historyTitle.textContent = 'Airport board';
    switchEl.innerHTML = [['isb', esc(isbName)], ['dep', `${depName} departures`]].map(([k, label]) =>
      `<button type="button" data-board="${k}" class="${k === board ? 'is-on' : ''}" aria-pressed="${k === board}">${label}</button>`).join('');
    switchEl.hidden = false;

    if (board === 'dep') {
      const row     = dep.row;
      const stale   = !row.departure_checked || minutesSince(row.departure_checked) > STALE_MINUTES;
      const checked = row.departure_checked ? esc(timeAgo(row.departure_checked)) : 'unknown';
      boardMetaEl.innerHTML = `${depName} board · checked ${checked}${stale ? ` · <span class="board-stale">may be out of date</span>` : ''}`;
      boardMetaEl.hidden = false;
      isbCheckedEl.hidden = true;   // Islamabad's "Last checked" would read as Lahore's
      renderSnapshots(dep.snaps);
    } else {
      boardMetaEl.hidden = true; boardMetaEl.innerHTML = '';
      isbCheckedEl.hidden = false;
      renderSnapshots(isbSnaps);
    }
  }

  // ===== FETCH SCRAPER FRESHNESS =====
  async function fetchFreshness() {
    try {
      let { data, error } = await client
        .from('origin_scraper_status')
        .select('last_run')
        .eq('scraper_id', 'paa_origin')
        .single();
      if (error || !data) {
        ({ data, error } = await client.from('scraper_status').select('last_run').eq('id', 1).single());
      }

      if (error || !data) return;
      lastScrapeTime = data.last_run;
      if (lastRefreshedEl) {
        lastRefreshedEl.textContent = `Last checked: ${timeAgo(data.last_run)}`;
      }
    } catch (e) {
      // non-critical
    }
  }

  // ===== FETCH SNAPSHOTS & RENDER =====
  async function fetchAndRender() {
    try {
      // Reads the origin-based view; if it is missing or errors, falls back to the original table
      const loadSnaps = (table) => {
        let q = client.from(table).select('*')
          .eq('flight_number', flightNumber)
          .eq('scheduled_date', date);
        if (typeParam === 'Arrival' || typeParam === 'Departure') q = q.eq('type', typeParam);
        return q.order('scraped_at', { ascending: true });
      };
      let { data: allSnaps, error } = await loadSnaps('isb_snapshots_view');
      if (error) {
        console.warn('isb_snapshots_view unavailable, using flight_snapshots', error);
        ({ data: allSnaps, error } = await loadSnaps('flight_snapshots'));
      }

      if (error) throw error;

      // Old links have no type: keep to the direction of the first record so the two never mix
      const snapshots = (typeParam || !allSnaps.length)
        ? allSnaps
        : allSnaps.filter(s => s.type === allSnaps[0].type);

      if (!snapshots.length) {
        flightInfoDiv.innerHTML = '<p>No history available for this flight.</p>';
        document.getElementById('snapshot-controls').style.display = 'none';
        return;
      }

      // Flight header: who, when, route, then Islamabad's current board line (re-drawn each refresh)
      const first     = snapshots[0];
      const latest    = snapshots[snapshots.length - 1];
      const isArrival = first.type === 'Arrival';
      const airline   = getAirlineInfo(flightNumber);
      const city      = display(latest.city || first.city);
      flightInfoDiv.innerHTML = `
        <div class="fi-head">
          <p class="fi-eyebrow">
            ${airline.name !== 'Airline' ? `<span>${esc(airline.name)}</span> · ` : ''}<span id="flight-date">${prettyDate(first.scheduled_date)}</span>
          </p>
          <p class="fi-number" id="flight-number">${display(first.flight_number)}</p>
          <p class="fi-route" id="flight-city">${isArrival ? `${city} <span aria-hidden="true">→</span><span class="sr-only"> to </span> Islamabad` : `Islamabad <span aria-hidden="true">→</span><span class="sr-only"> to </span> ${city}`}</p>
          <p class="sr-only" id="flight-type">${display(first.type)}</p>
        </div>
        ${legHtml({
          label: `Islamabad · ${isArrival ? 'arrival' : 'departure'}`,
          st: latest.st, et: latest.et, status: latest.status,
        })}
      `;

      // Always ensure modal exists in DOM, then re-inject button with fresh snapshot closure
      injectShareModal();
      const existingBtn = document.getElementById('share-btn');
      if (existingBtn) existingBtn.remove();
      injectShareButton(snapshots);

      isbSnaps        = snapshots;
      isArrivalFlight = isArrival;
      [dep, adb]      = await Promise.all([fetchDeparture(first.type), fetchAdb(first.type)]);
      flightInfoDiv.insertAdjacentHTML('beforeend', checkLinksHtml(snapshots));
      renderNotice(latest);
      renderBoard();

    } catch (err) {
      console.error(err);
      if (flightInfoDiv.querySelector('#flight-number') === null) {
        flightInfoDiv.innerHTML = '<p>Error loading flight history. Please try again.</p>';
      }
    }
  }

  // ===== INIT =====
  await fetchAndRender();
  await fetchFreshness();

  setInterval(async () => {
    await fetchAndRender();
    await fetchFreshness();
  }, 5 * 60 * 1000);

});
