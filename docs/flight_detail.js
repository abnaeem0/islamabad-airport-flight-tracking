document.addEventListener('DOMContentLoaded', async () => {

  // ===== URL PARAM VALIDATION =====
  const urlParams    = new URLSearchParams(window.location.search);
  const flightNumber = urlParams.get('flight');
  const date         = urlParams.get('date');
  const flightInfoDiv = document.getElementById('flight-info');

  if (!flightNumber || !date) {
    flightInfoDiv.innerHTML = '<p>Invalid page link. No flight or date specified. <a href="index.html">Go back to search</a>.</p>';
    document.getElementById('snapshot-controls').style.display = 'none';
    document.getElementById('snapshot-table').style.display   = 'none';
    return;
  }

  // ===== SUPABASE =====
  const supabaseUrl = 'https://sbaweaytsmdmhaclgcwr.supabase.co';
  const supabaseKey = 'sb_publishable_PBY7Y_HM60Ijqw9j6iOGeg_XqLDI7SS';
  const client      = supabase.createClient(supabaseUrl, supabaseKey);

  // ===== AIRLINE LOOKUP =====
  // Keyed by the IATA prefix of the flight number
  const AIRLINE_INFO = {
    PK: {
      name:      'Pakistan Intl (PIA)',
      contact:   '111-786-786',
      website:   'https://www.piac.com.pk',
      statusUrl: 'https://www.piac.com.pk/travel-information/flight-status',
    },
    PA: {
      name:      'AirBlue',
      contact:   '111-247-258',
      website:   'https://www.airblue.com',
      statusUrl: 'https://www.airblue.com/bookings/flight_status.aspx',
    },
    '9P': {
      name:      'Fly Jinnah',
      contact:   '021-111-000-035',
      website:   'https://www.flyjinnah.com',
      statusUrl: 'https://www.flyjinnah.com/en/help/flight-status',
    },
    PF: {
      name:      'AirSial',
      contact:   '021-111-247-742',
      website:   'https://www.airsial.com',
      statusUrl: 'https://www.airsial.com/flight-status',
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

  function formatPKT(dateStr) {
    const utcDate = new Date(dateStr + 'Z');
    return utcDate.toLocaleString('en-GB', {
      timeZone:   'Asia/Karachi',
      year:       'numeric',
      month:      '2-digit',
      day:        '2-digit',
      hour:       '2-digit',
      minute:     '2-digit',
    });
  }

  function display(val) {
    return val ?? '—';
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

        <pre id="share-text-preview"></pre>

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
      const text = document.getElementById('share-text-preview').textContent;
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
      const text   = encodeURIComponent(document.getElementById('share-text-preview').textContent);
      window.open(`https://wa.me/${number}?text=${text}`, '_blank');
    });
  }

  function openShareModal(text) {
    document.getElementById('share-text-preview').textContent = text;
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
    const lastUpd = formatPKT(latest.scraped_at); // full date + time in PKT

    // FlightStats link: /flight-tracker/IATA/NUMBER?year=YYYY&month=M&date=D
    const letters  = flightNumber.replace(/[^A-Za-z]/g, '');
    const digits   = flightNumber.replace(/[^0-9]/g, '');
    const [yr, mo, dy] = date.split('-');
    const fsUrl = `https://www.flightstats.com/v2/flight-tracker/${letters}/${digits}?year=${yr}&month=${parseInt(mo)}&date=${parseInt(dy)}`;

    const pageUrl = window.location.href;

    const lines = [
      `✈️ Flight Update`,
      `Flight: ${flightNumber}`,
      `Route: ${route}`,
      `⏰ Departure: ${dep}`,
      `📊 Status: ${status}`,
      `🕒 Last update: ${lastUpd} PKT`,
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

    flightInfoDiv.appendChild(btn);
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

  // ===== FETCH SCRAPER FRESHNESS =====
  async function fetchFreshness() {
    try {
      const { data, error } = await client
        .from('scraper_status')
        .select('last_run')
        .eq('id', 1)
        .single();

      if (error || !data) return;
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
      const { data: snapshots, error } = await client
        .from('flight_snapshots')
        .select('*')
        .eq('flight_number', flightNumber)
        .eq('scheduled_date', date)
        .order('scraped_at', { ascending: true });

      if (error) throw error;

      if (!snapshots.length) {
        flightInfoDiv.innerHTML = '<p>No history available for this flight.</p>';
        document.getElementById('snapshot-controls').style.display = 'none';
        return;
      }

      // Populate flight header (only once)
      if (flightInfoDiv.querySelector('#flight-number') === null) {
        const first = snapshots[0];
        flightInfoDiv.innerHTML = `
          <p id="flight-number">Flight: ${display(first.flight_number)}</p>
          <p id="flight-date">Date: ${display(first.scheduled_date)}</p>
          <p id="flight-type">Type: ${display(first.type)}</p>
          <p id="flight-city">${first.type === 'Arrival' ? 'From' : 'To'}: ${display(first.city)}</p>
        `;
      }

      // Always ensure modal exists in DOM, then re-inject button with fresh snapshot closure
      injectShareModal();
      const existingBtn = document.getElementById('share-btn');
      if (existingBtn) existingBtn.remove();
      injectShareButton(snapshots);

      renderSnapshots(snapshots);

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
