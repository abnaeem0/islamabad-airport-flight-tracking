document.addEventListener('DOMContentLoaded', async () => {

  // ===== URL PARAM VALIDATION =====
  const urlParams    = new URLSearchParams(window.location.search);
  const flightNumber = urlParams.get('flight');
  const date         = urlParams.get('date');
  const typeParam    = urlParams.get('type');   // 'Arrival' | 'Departure' | null (old links)
  // History layout test (BL-038): only links carrying ?layout= show the switcher; everyone else sees 'separate'
  const layoutParam  = urlParams.get('layout');
  const LAYOUTS      = { separate: 'Separate', list: 'One list', columns: 'Side by side' };
  const layout       = LAYOUTS[layoutParam] ? layoutParam : 'separate';
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
  // Keyed by the IATA prefix of the flight number
  const AIRLINE_INFO = {
    PK: {
      name:      'Pakistan Intl (PIA)',
      contact:   '111-786-786',
      statusUrl: 'https://book-pia.crane.aero/ibe/flightStatus?&tab=route',
      fsCode:    'PK',
      fsSuffix:  '',
    },
    PA: {
      name:      'AirBlue',
      contact:   '111-247-258',
      statusUrl: 'https://www.airblue.com/bookings/flight_status.aspx',
      fsCode:    'ABQ',
      fsSuffix:  '',
    },
    '9P': {
      name:      'Fly Jinnah',
      contact:   '021-111-000-035',
      statusUrl: 'https://www.flyjinnah.com/en/manage/flight-status/check-flight-status',
      fsCode:    'FJL',
      fsSuffix:  '',
    },
    PF: {
      name:      'AirSial',
      contact:   '021-111-247-742',
      statusUrl: 'https://www.airsial.com/manage-reservation',
      fsCode:    'PF',
      fsSuffix:  '*',
    },
  };

  function getAirlineInfo(flightNum) {
    // Try 2-char prefix first, then 1-char
    const letters = flightNum.replace(/[^A-Za-z0-9]/g, '');
    const prefix2 = letters.slice(0, 2).toUpperCase();
    const prefix1 = letters.slice(0, 1).toUpperCase();
    return AIRLINE_INFO[prefix2] || AIRLINE_INFO[prefix1] || {
      name:      'Airline',
      contact:   'N/A',
      website:   '#',
      statusUrl: '#',
    };
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

  function statusPill(status, small) {
    return `<span class="status-pill status-pill--${statusTone(status)}${small ? ' status-pill--sm' : ''}">${display(status || 'No status')}</span>`;
  }

  // One airport's line: big time (expected if it differs, else scheduled), status, and the sub-line
  function legHtml({ label, st, et, status, small, extraDate }) {
    const d       = minutesDiff(st, et);
    const hasEt   = et != null && String(et).trim() !== '' && String(et).trim() !== String(st || '').trim();
    const bigTime = hasEt ? et : st;
    const kind    = hasEt ? 'Expected' : 'Scheduled';
    const dtxt    = diffText(d);
    const sub = hasEt
      ? `Expected · scheduled ${display(st)}${extraDate ? ' (' + display(extraDate) + ')' : ''}${dtxt ? ` · <strong class="delta delta--${d < 0 ? 'early' : 'late'}">${esc(dtxt)}</strong>` : ''}`
      : `${kind}${extraDate ? ' · ' + display(extraDate) : ''} · no expected time posted`;
    return `
      <div class="leg ${small ? 'leg--secondary' : 'leg--primary'}">
        <p class="leg-label">${label}</p>
        <div class="leg-main">
          <span class="leg-time">${display(bigTime)}</span>
          ${statusPill(status, small)}
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

        <div id="share-text-preview" role="region" aria-label="Share message preview"></div>

        <div id="share-actions">
          <button id="share-copy-btn" class="share-btn share-btn--primary">
            <span id="share-copy-icon">📋</span> Copy
          </button>

          <!-- WhatsApp -->
          <div id="share-wa-row">
            <label for="share-wa-input">WhatsApp number</label>
            <div id="share-wa-input-row">
              <span id="share-wa-prefix">+92</span>
              <input id="share-wa-input" type="tel" maxlength="10" placeholder="3001234567" inputmode="numeric">
              <button id="share-wa-btn" class="share-btn share-btn--whatsapp" aria-label="Send on WhatsApp">
                <svg id="share-wa-icon" viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347z"/><path d="M12 0C5.373 0 0 5.373 0 12c0 2.124.558 4.122 1.532 5.862L.057 23.428a.75.75 0 0 0 .916.916l5.566-1.475A11.944 11.944 0 0 0 12 24c6.627 0 12-5.373 12-12S18.627 0 12 0zm0 21.75a9.694 9.694 0 0 1-4.953-1.357l-.355-.211-3.683.975.993-3.585-.232-.369A9.695 9.695 0 0 1 2.25 12C2.25 6.615 6.615 2.25 12 2.25S21.75 6.615 21.75 12 17.385 21.75 12 21.75z"/></svg>
                Open WhatsApp
              </button>
            </div>
          </div>

          <!-- SMS stub (hidden, enable later) -->
          <!-- <button id="share-sms-btn" class="share-btn share-btn--secondary">📱 SMS</button> -->

          <!-- Email stub (hidden, enable later) -->
          <!-- <button id="share-email-btn" class="share-btn share-btn--secondary">✉️ Email</button> -->
        </div>
      </div>
    `;
    document.body.appendChild(modal);

    // Close handlers
    document.getElementById('share-modal-close').addEventListener('click', closeShareModal);
    document.getElementById('share-modal-backdrop').addEventListener('click', closeShareModal);
    document.addEventListener('keydown', e => { if (e.key === 'Escape') closeShareModal(); });

    // Copy button
    document.getElementById('share-copy-btn').addEventListener('click', () => {
      const text = document.getElementById('share-text-preview').innerText;
      navigator.clipboard.writeText(text).then(() => {
        const btn = document.getElementById('share-copy-btn');
        btn.textContent = '✅ Copied!';
        setTimeout(() => { btn.innerHTML = '<span>📋</span> Copy'; }, 2000);
      });
    });

    // WhatsApp button
    document.getElementById('share-wa-btn').addEventListener('click', () => {
      const raw = document.getElementById('share-wa-input').value.trim().replace(/\D/g, '');
      if (!raw) {
        document.getElementById('share-wa-input').focus();
        return;
      }
      const number = '92' + raw.replace(/^0+/, '');
      const text   = encodeURIComponent(document.getElementById('share-text-preview').innerText);
      window.open(`https://wa.me/${number}?text=${text}`, '_blank');
    });
  }

  function renderPreview(text) {
    const el = document.getElementById('share-text-preview');
    // Linkify URLs, keep everything else as escaped text
    const urlRegex = /(https?:\/\/[^\s]+)/g;
    const html = text
      .split('\n')
      .map(line => {
        const escaped = line.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
        return escaped.replace(urlRegex, url =>
          `<a href="${url}" target="_blank" rel="noopener">${url}</a>`
        );
      })
      .join('\n');
    el.innerHTML = html;
  }

  function openShareModal(text) {
    renderPreview(text);
    document.getElementById('share-modal').classList.add('share-modal--open');
    document.body.style.overflow = 'hidden';
  }

  function closeShareModal() {
    document.getElementById('share-modal').classList.remove('share-modal--open');
    document.body.style.overflow = '';
  }

  // Build the share message text from current page state
  function buildShareText(snapshots) {
    const latest  = snapshots[snapshots.length - 1];
    const airline = getAirlineInfo(flightNumber);

    const city      = latest.city || '—';
    const isArrival = (latest.type || '').toLowerCase() === 'arrival';
    const route     = isArrival ? `${city} → Islamabad` : `Islamabad → ${city}`;

    const dep     = latest.st ? `${latest.st} (${date})` : '—';
    const status  = latest.status || '—';
    const lastUpd = lastScrapeTime ? formatPKT(lastScrapeTime) : formatPKT(latest.scraped_at);

    // FlightStats link: strip the IATA prefix to get pure flight digits
    // e.g. 9P734 -> prefix is '9P' (2 chars) -> digits '734'
    //      PF125 -> prefix is 'PF' (2 chars) -> digits '125'
    const iataKey  = AIRLINE_INFO[flightNumber.slice(0,2).toUpperCase()] ? 2 : 1;
    const digits   = flightNumber.slice(iataKey).replace(/[^0-9]/g, '');
    const fsCode   = airline.fsCode   || flightNumber.replace(/[^A-Za-z]/g, '');
    const fsSuffix = airline.fsSuffix || '';
    const [yr, mo, dy] = date.split('-');
    const fsUrl = `https://www.flightstats.com/v2/flight-tracker/${fsCode}${fsSuffix}/${digits}?year=${yr}&month=${parseInt(mo)}&date=${parseInt(dy)}`;

    const pageUrl = window.location.href;

    const lines = [
      `✈️ Flight Update`,
      `Flight: ${flightNumber}`,
      `Route: ${route}`,
      `⏰ ${isArrival ? 'Arrival' : 'Departure'}: ${dep}`,
      `📊 Status: ${status}`,
      `🕒 Last scraped: ${lastUpd} PKT`,
      `────────────────`,
      `🔎 Live verification:`,
      `FlightStats: ${fsUrl}`,
    ];

    if (airline.statusUrl && airline.statusUrl !== '#') {
      lines.push(`🏢 Airline status:`);
      lines.push(`${airline.name}: ${airline.statusUrl}`);
    }
    if (airline.contact && airline.contact !== 'N/A') {
      lines.push(`📞 Helpline: ${airline.contact}`);
    }

    lines.push(`────────────────`);
    lines.push(`🛫 Airport status check:`);
    lines.push(pageUrl);

    return lines.join('\n');
  }

  // Inject the Share button into #flight-info (called after header renders)
  function injectShareButton(snapshots) {
    const btn = document.createElement('button');
    btn.id        = 'share-btn';
    btn.className = 'share-trigger-btn';
    btn.innerHTML = '🔗 Share Flight';
    btn.addEventListener('click', () => {
      const text = buildShareText(snapshots);
      openShareModal(text);
    });

    (flightInfoDiv.querySelector('.fi-head') || flightInfoDiv).appendChild(btn);
  }

  // ===== RENDER SNAPSHOTS =====
  function renderSnapshots(snapshots) {
    tbody.innerHTML = '';

    if (!snapshots.length) {
      tbody.innerHTML = '<tr><td colspan="5">No changes recorded for this flight.</td></tr>';
      return;
    }

    snapshots.forEach(s => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${formatPKT(s.scraped_at)}</td>
        <td>${display(s.st)}</td>
        <td>${display(s.et)}</td>
        <td>${display(s.status)}</td>
        <td>${changeLabel(s.change_type)}</td>
      `;
      tbody.appendChild(tr);
    });
  }

  // ===== HISTORY LAYOUTS (BL-038 test) =====
  const historyTitle = document.querySelector('.section-title');
  const tableEl      = document.getElementById('snapshot-table');
  let   altHistory   = null;

  function shortPKT(dateStr) {
    const n = dateStr.endsWith('Z') || dateStr.includes('+') ? dateStr : dateStr + 'Z';
    return new Date(n).toLocaleString('en-GB', {
      timeZone: 'Asia/Karachi', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false,
    });
  }

  function changeText(t) {
    return { new: 'First seen', status_change: 'Status changed', time_change: 'Time changed',
             city_change: 'City changed', dropped: 'Dropped from board' }[t] || 'Update';
  }

  // "Advanced · expected 15:25 · sched 15:55"
  function boardLine(s) {
    const hasEt = s.et != null && String(s.et).trim() !== '' && String(s.et).trim() !== String(s.st || '').trim();
    return [s.status ? display(s.status) : null,
            hasEt ? `expected ${display(s.et)}` : null,
            `sched ${display(s.st)}`].filter(Boolean).join(' · ');
  }

  function renderSwitcher(hasDep) {
    let sw = document.getElementById('layout-switch');
    if (!layoutParam) { if (sw) sw.remove(); return; }
    if (!sw) {
      sw = document.createElement('nav');
      sw.id = 'layout-switch';
      sw.setAttribute('aria-label', 'History layout (test)');
      historyTitle.parentNode.insertBefore(sw, historyTitle);
    }
    const link = key => { const u = new URLSearchParams(window.location.search); u.set('layout', key); return '?' + u.toString(); };
    sw.innerHTML = `<span class="ls-label">Test view</span>` + Object.entries(LAYOUTS).map(([k, v]) =>
      `<a href="${esc(link(k))}" class="${k === layout ? 'is-on' : ''}"${k === layout ? ' aria-current="true"' : ''}>${v}</a>`).join('')
      + (hasDep ? '' : `<span class="ls-hint">No departure-airport history for this flight, so all three look alike.</span>`);
  }

  function renderHistory(isbSnaps, dep, isArrival) {
    const depSnaps = dep && dep.snaps.length ? dep.snaps : null;
    renderSwitcher(!!depSnaps);

    if (layout === 'separate' || !depSnaps) {
      if (altHistory) { altHistory.remove(); altHistory = null; }
      tableEl.style.display = '';
      historyTitle.textContent = `Islamabad board history`;
      renderSnapshots(isbSnaps);
      return;
    }

    tableEl.style.display = 'none';
    if (!altHistory) {
      altHistory = document.createElement('div');
      altHistory.id = 'history-alt';
      tableEl.parentNode.insertBefore(altHistory, tableEl.nextSibling);
    }
    const depName = display(dep.airport);
    historyTitle.innerHTML = `Flight history · ${depName} and Islamabad`;

    const rows = [
      ...depSnaps.map(s => ({ ...s, side: 'dep' })),
      ...isbSnaps.map(s => ({ ...s, side: 'isb' })),
    ].sort((a, b) => new Date(a.scraped_at.endsWith('Z') || a.scraped_at.includes('+') ? a.scraped_at : a.scraped_at + 'Z')
                   - new Date(b.scraped_at.endsWith('Z') || b.scraped_at.includes('+') ? b.scraped_at : b.scraped_at + 'Z'));

    if (layout === 'list') {
      altHistory.innerHTML = `<ol class="hl-list">${rows.map(r => `
        <li class="hl-row hl-row--${r.side}">
          <span class="hl-time">${esc(shortPKT(r.scraped_at))}</span>
          <div class="hl-body">
            <p><span class="hl-airport">${r.side === 'isb' ? 'Islamabad' : depName}</span>
               <span class="hl-change">${changeText(r.change_type)}</span></p>
            <p class="hl-detail">${boardLine(r)}</p>
          </div>
        </li>`).join('')}</ol>`;
    } else {
      altHistory.innerHTML = `
        <table class="hc-table">
          <thead><tr><th>Seen at</th><th class="hc-dep">${depName}<small>departure board</small></th><th class="hc-isb">Islamabad<small>${isArrival ? 'arrival' : 'departure'} board</small></th></tr></thead>
          <tbody>${rows.map(r => `
            <tr>
              <td class="hc-time">${esc(shortPKT(r.scraped_at))}</td>
              <td class="hc-dep">${r.side === 'dep' ? `<span class="hc-change">${changeText(r.change_type)}</span>${boardLine(r)}` : ''}</td>
              <td class="hc-isb">${r.side === 'isb' ? `<span class="hc-change">${changeText(r.change_type)}</span>${boardLine(r)}` : ''}</td>
            </tr>`).join('')}</tbody>
        </table>`;
    }
  }

  // ===== DEPARTURE AIRPORT BOX (domestic arrivals only) =====
  // Reads the view isb_arrival_origin_view (BL-010B). If the view is missing, empty or
  // unmatched, the box stays hidden and the rest of the page is unaffected.
  const originBox = document.getElementById('origin-box');
  const STALE_MINUTES = 60;

  function hideOriginBox() {
    if (originBox) { originBox.hidden = true; originBox.innerHTML = ''; }
  }

  function minutesSince(dateStr) {
    return (Date.now() - new Date(dateStr).getTime()) / 60000;
  }

  // isbLeg: latest Islamabad snapshot, used to spot when the two boards disagree
  function renderOriginBox(row, isbLeg, departedAt) {
    if (!originBox) return;
    if (!row || row.match_state !== 'matched') { hideOriginBox(); return; }

    const stale = !row.departure_checked || minutesSince(row.departure_checked) > STALE_MINUTES;
    const checked = row.departure_checked
      ? `${esc(timeAgo(row.departure_checked))} (${esc(formatPKT(row.departure_checked))} PKT)`
      : 'unknown';
    const overnight = row.departure_date && row.departure_date !== row.arrival_date;
    const source = row.departure_source === 'paa' ? 'Pakistan Airports Authority' : row.departure_source;

    // Islamabad has moved the arrival time, but the departure airport shows nothing beyond the schedule
    const isbShift   = isbLeg ? minutesDiff(isbLeg.st, isbLeg.et) : null;
    const depHasEt   = row.departure_et != null && String(row.departure_et).trim() !== ''
                       && String(row.departure_et).trim() !== String(row.departure_st || '').trim();
    const depLeft    = /depart|airborne|take/i.test(row.departure_status || '');
    const disagree   = isbShift !== null && Math.abs(isbShift) >= 5 && !depHasEt && !depLeft;

    originBox.innerHTML = `
      ${legHtml({
        label: `${display(row.departure_airport)} · departure board`,
        st: row.departure_st, et: row.departure_et, status: row.departure_status,
        small: true, extraDate: overnight ? row.departure_date : null,
      })}
      ${departedAt ? `<p class="leg-note">This board first showed it departed at ${esc(formatPKT(departedAt))} PKT. We check about every 15 minutes, so it left at or before then.</p>` : ''}
      ${disagree ? `<p class="leg-note leg-note--warn">Islamabad's board has changed the arrival time (${esc(diffText(isbShift))}), but ${display(row.departure_airport)}'s board has not posted a departure update. The two airports update their boards separately, so they can disagree for a while.</p>` : ''}
      <p class="leg-meta">Source: ${display(source)} · checked ${checked}</p>
      ${stale ? `<p class="leg-note leg-note--warn">${display(row.departure_airport)} information may be out of date.</p>` : ''}
    `;
    originBox.hidden = false;
  }

  // The departure board's change history for the paired leg (oldest first); [] if unreadable
  async function fetchDepHistory(row) {
    try {
      const { data, error } = await client
        .from('origin_snapshots')
        .select('scraped_at,status,st,et,change_type')
        .eq('flight_number', row.flight_number)
        .eq('scheduled_date', row.departure_date)
        .eq('type', 'Departure')
        .eq('source_airport', row.departure_airport)
        .order('scraped_at', { ascending: true });
      return (error || !data) ? [] : data;
    } catch (e) {
      return [];
    }
  }

  // Returns { airport, snaps } for a paired domestic arrival, otherwise null
  async function fetchOriginBox(flightType, isbLeg) {
    if (flightType !== 'Arrival') { hideOriginBox(); return null; }
    try {
      const { data, error } = await client
        .from('isb_arrival_origin_view')
        .select('*')
        .eq('flight_number', flightNumber)
        .eq('arrival_date', date)
        .limit(1);
      if (error || !data || !data.length) { hideOriginBox(); return null; }
      const row = data[0];
      if (row.match_state !== 'matched') { hideOriginBox(); return null; }
      const depSnaps   = await fetchDepHistory(row);
      const firstGone  = depSnaps.find(d => /depart/i.test(d.status || ''));
      renderOriginBox(row, isbLeg, firstGone ? firstGone.scraped_at : null);
      return { airport: row.departure_airport, snaps: depSnaps };
    } catch (e) {
      hideOriginBox();
      return null;
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
          st: latest.st, et: latest.et, status: latest.status, small: false,
        })}
      `;

      // Always ensure modal exists in DOM, then re-inject button with fresh snapshot closure
      injectShareModal();
      const existingBtn = document.getElementById('share-btn');
      if (existingBtn) existingBtn.remove();
      injectShareButton(snapshots);

      const dep = await fetchOriginBox(first.type, latest);
      renderHistory(snapshots, dep, isArrival);

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
