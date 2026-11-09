document.addEventListener('DOMContentLoaded', () => {

  // If the Supabase script is blocked or slow, keep the rest of the page working
  // (saved searches still show); only loading flights is disabled.
  const supabaseReady = typeof supabase !== 'undefined';
  if (!supabaseReady) console.error('Supabase failed to load.');

  const supabaseUrl = 'https://sbaweaytsmdmhaclgcwr.supabase.co';
  const supabaseKey = 'sb_publishable_PBY7Y_HM60Ijqw9j6iOGeg_XqLDI7SS';
  const client = supabaseReady ? supabase.createClient(supabaseUrl, supabaseKey) : null;

  const { AIRLINE_INFO, splitFlightNumber, airlineSearchText, norm } = window.Airlines;

  const searchInput     = document.getElementById('flight-search');
  const suggestBox      = document.getElementById('search-suggest');
  const dateInput       = document.getElementById('flight-date');
  const dateLabel       = document.getElementById('date-label');
  const typeSeg         = document.getElementById('type-seg');
  const routeSeg        = document.getElementById('route-seg');
  const metaEl          = document.getElementById('results-meta');
  const resultsDiv      = document.getElementById('search-results');
  const historyList     = document.getElementById('history-list');
  const clearHistoryBtn = document.getElementById('clear-history-btn');

  // ===== STATE =====
  // One search (BL-048): the whole day is loaded once; the box and the two toggles only filter it.
  const state = { date: todayPKT(), q: '', type: '', route: '' };
  let dayRows   = [];      // flights for state.date
  let loadedFor = null;    // date dayRows belongs to
  let loadToken = 0;       // ignores answers to an older day's request
  let lastRun   = null;    // collector's last run (freshness line)

  // A search kept in the address (BL-042): restored on Back, reload, or a shared search link.
  // Today's date is left out of the address, so a reload tomorrow opens the new today; any other
  // day stays pinned. Old links may carry city= (the city filter was folded into the box).
  const startParams = new URLSearchParams(window.location.search);
  if (/^\d{4}-\d{2}-\d{2}$/.test(startParams.get('date') || '')) state.date = startParams.get('date');
  state.q     = (startParams.get('q') || startParams.get('city') || '').slice(0, 40);
  state.type  = ['Arrival', 'Departure'].includes(startParams.get('type')) ? startParams.get('type') : '';
  state.route = ['International', 'Domestic'].includes(startParams.get('route')) ? startParams.get('route') : '';

  // Back, Forward or reload: return to the same place in the list (not on a fresh visit)
  let restoreScroll = false;
  try {
    const nav = performance.getEntriesByType('navigation')[0];
    restoreScroll = !!nav && (nav.type === 'back_forward' || nav.type === 'reload');
  } catch (e) { /* older browsers: start at the top */ }

  // ===== UTILS =====
  function todayPKT() {
    return new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Karachi' });
  }

  function shiftDate(iso, days) {
    const [y, m, d] = iso.split('-').map(Number);
    return new Date(Date.UTC(y, m - 1, d + days)).toISOString().slice(0, 10);
  }

  // "Mon 9 Nov"
  function dayName(iso) {
    const [y, m, d] = iso.split('-').map(Number);
    return new Date(Date.UTC(y, m - 1, d))
      .toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' })
      .replace(',', '');
  }

  function relDayName(iso) {
    const t = todayPKT();
    if (iso === t) return `Today · ${dayName(iso)}`;
    if (iso === shiftDate(t, -1)) return `Yesterday · ${dayName(iso)}`;
    if (iso === shiftDate(t, 1)) return `Tomorrow · ${dayName(iso)}`;
    return dayName(iso);
  }

  function nowMinutesPKT() {
    const [h, m] = new Date().toLocaleTimeString('en-GB', { timeZone: 'Asia/Karachi', hour12: false })
      .split(':').map(Number);
    return (h % 24) * 60 + m;
  }

  function timeToMinutes(t) {
    const m = /^(\d{1,2}):(\d{2})/.exec(t || '');
    return m ? (+m[1]) * 60 + (+m[2]) : 24 * 60;   // no time: end of the list
  }

  // Escape text before it is placed into innerHTML (values come from a third-party feed)
  function esc(val) {
    return String(val).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
                      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function timeAgo(iso) {
    const mins = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
    if (!isFinite(mins)) return '';
    if (mins < 1) return 'just now';
    if (mins < 60) return `${mins} min ago`;
    const h = Math.floor(mins / 60);
    return h < 48 ? `${h} hour${h > 1 ? 's' : ''} ago` : `${Math.floor(h / 24)} days ago`;
  }

  function airlineCode(flightNumber) {
    return splitFlightNumber(flightNumber).code;
  }

  // --- Airline Logos ---
  const LOCAL_AIRLINES = ['PK', 'PF', '9P', 'PA'];

  function getLogoUrl(code) {
    if (LOCAL_AIRLINES.includes(code)) return `logos/${code.toLowerCase()}.png`;
    return `https://pics.avs.io/96/96/${encodeURIComponent(code)}.png`;   // 96 px for sharp 48 px tiles
  }

  // Status colour (words, not just colour, are always shown)
  function statusClass(status) {
    const s = String(status || '').toLowerCase();
    if (/cancel|dropped|divert/.test(s))                        return 'st-bad';
    if (/delay|late|reschedul/.test(s))                         return 'st-warn';
    if (/landed|arrived|departed|airborne/.test(s))             return 'st-done';
    if (/board|gate|final|check|baggage|advanced|early/.test(s)) return 'st-info';
    return 'st-plain';
  }

  // ===== CONTROLS =====
  function setSeg(seg, value) {
    seg.querySelectorAll('button').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.value === value)));
  }

  function syncControls() {
    searchInput.value = state.q;
    dateInput.value   = state.date;
    dateLabel.textContent = relDayName(state.date);
    setSeg(typeSeg, state.type);
    setSeg(routeSeg, state.route);
  }

  typeSeg.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    state.type = b.dataset.value; setSeg(typeSeg, state.type); render(); scheduleSave();
  });
  routeSeg.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    state.route = b.dataset.value; setSeg(routeSeg, state.route); render(); scheduleSave();
  });

  document.getElementById('day-prev').addEventListener('click', () => changeDate(shiftDate(state.date, -1)));
  document.getElementById('day-next').addEventListener('click', () => changeDate(shiftDate(state.date, 1)));
  dateInput.addEventListener('change', () => {
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateInput.value)) changeDate(dateInput.value);
  });
  // Desktop browsers only open the calendar from its small icon; open it from the whole pill
  dateInput.addEventListener('click', () => { try { dateInput.showPicker(); } catch (e) { /* not supported */ } });

  function changeDate(iso) {
    state.date = iso;
    syncControls();
    loadDay();
    scheduleSave();
  }

  searchInput.addEventListener('input', () => {
    state.q = searchInput.value.slice(0, 40);
    render();
    showSuggestions();
    scheduleSave();
  });
  searchInput.addEventListener('focus', showSuggestions);
  searchInput.addEventListener('keydown', e => {
    if (e.key === 'Escape') hideSuggestions();
    if (e.key === 'Enter') { hideSuggestions(); searchInput.blur(); }
  });
  // Close suggestions when tapping elsewhere (mousedown on a suggestion runs before blur)
  document.addEventListener('click', e => {
    if (!e.target.closest('#search-box-wrap')) hideSuggestions();
  });

  // ===== SUGGESTIONS =====
  // Airlines and cities in the day's list that match what was typed, with counts
  function showSuggestions() {
    const qn = norm(state.q);
    if (qn.length < 2 || !dayRows.length) return hideSuggestions();
    const airlines = new Map(), cities = new Map();
    for (const f of dayRows) {
      const code = airlineCode(f.flight_number);
      if (airlineSearchText(code).includes(qn)) airlines.set(code, (airlines.get(code) || 0) + 1);
      if (f.city && norm(f.city).includes(qn))  cities.set(f.city, (cities.get(f.city) || 0) + 1);
    }
    const items = [
      ...[...airlines].map(([code, n]) => ({ kind: 'airline', code, text: (AIRLINE_INFO[code] || {}).name || code, n })),
      ...[...cities].map(([city, n]) => ({ kind: 'city', text: city, n })),
    ].sort((a, b) => b.n - a.n).slice(0, 6);
    // Nothing to offer, or the box already says exactly the one suggestion
    if (!items.length || (items.length === 1 && norm(items[0].text) === qn)) return hideSuggestions();
    suggestBox.innerHTML = items.map(it => `
      <button type="button" class="sug-item" data-text="${esc(it.text)}">
        ${it.kind === 'airline'
          ? `<span class="sug-code">${esc(it.code)}</span>`
          : '<span class="sug-code sug-city" aria-hidden="true">⌖</span>'}
        <span class="sug-text">${esc(it.text)}</span>
        <span class="sug-n">${it.kind === 'city' ? 'city · ' : ''}${it.n} flight${it.n > 1 ? 's' : ''}</span>
      </button>`).join('');
    suggestBox.hidden = false;
  }

  function hideSuggestions() { suggestBox.hidden = true; }

  suggestBox.addEventListener('click', e => {
    const b = e.target.closest('.sug-item'); if (!b) return;
    state.q = b.dataset.text;
    searchInput.value = state.q;
    hideSuggestions();
    render();
    scheduleSave(0);
  });

  // ===== LOADING =====
  async function loadDay() {
    const token = ++loadToken;
    const date  = state.date;
    if (!client) {
      resultsDiv.innerHTML = '<p class="error">Flights can’t be loaded right now because a required script did not load. Please reload the page.</p>';
      metaEl.textContent = '';
      return;
    }
    resultsDiv.innerHTML = '<p>Loading…</p>';
    metaEl.textContent = '';
    updateNowBtn();
    try {
      // Reads the origin-based view; if it is missing or errors, falls back to the original table
      const run = table => client.from(table).select('*').eq('scheduled_date', date);
      let { data, error } = await run('isb_flights_view');
      if (error) {
        console.warn('isb_flights_view unavailable, using flights table', error);
        ({ data, error } = await run('flights'));
      }
      if (error) throw error;
      if (token !== loadToken) return;      // the day was changed while loading
      dayRows   = Array.isArray(data) ? data : [];
      loadedFor = date;
      render();
      let y = null;
      if (restoreScroll) {                  // coming back from a flight: same place in the list
        restoreScroll = false;
        try { y = sessionStorage.getItem(scrollKey()); } catch (e) { /* ignore */ }
      }
      // instant: the page's smooth scrolling would otherwise animate from the top
      if (y !== null) window.scrollTo({ top: Number(y) || 0, behavior: 'instant' });
      updateNowBtn();
    } catch (err) {
      if (token !== loadToken) return;
      console.error(err);
      dayRows = []; loadedFor = null;
      resultsDiv.innerHTML = '<p class="error">Couldn’t load flights. Please try again.</p>';
      metaEl.textContent = '';
    }
  }

  async function loadFreshness() {
    if (!client) return;
    try {
      let { data, error } = await client.from('origin_scraper_status')
        .select('last_run').eq('scraper_id', 'paa_origin').single();
      if (error || !data) ({ data, error } = await client.from('scraper_status').select('last_run').eq('id', 1).single());
      if (!error && data && data.last_run) { lastRun = data.last_run; renderMeta(null); }
    } catch (e) { /* non-critical */ }
  }

  // ===== FILTER + RENDER =====
  function matches(f, qn) {
    if (!qn) return true;
    if (norm(f.flight_number).includes(qn)) return true;
    if (airlineSearchText(airlineCode(f.flight_number)).includes(qn)) return true;
    return norm(f.city).includes(qn);
  }

  let lastShown = 0;
  function renderMeta(count) {
    if (count !== null) lastShown = count;
    if (loadedFor !== state.date) return;
    const parts = [`${lastShown} flight${lastShown === 1 ? '' : 's'}`];
    if (lastRun) parts.push(`updated ${timeAgo(lastRun)}`);
    metaEl.textContent = parts.join(' · ');
  }

  function render() {
    saveSearchToAddress();
    if (loadedFor !== state.date) return;   // still loading this day
    const qn = norm(state.q);
    const shown = dayRows
      .filter(f => (!state.type || f.type === state.type) && (!state.route || f.nature === state.route) && matches(f, qn))
      .sort((a, b) => timeToMinutes(a.st) - timeToMinutes(b.st));
    renderMeta(shown.length);

    if (!dayRows.length) {
      resultsDiv.innerHTML = `<p>No flights found for ${esc(dayName(state.date))}.</p>`;
      return;
    }
    if (!shown.length) {
      resultsDiv.innerHTML = `<p>No flights match${state.q ? ` “${esc(state.q)}”` : ''} on ${esc(dayName(state.date))}.</p>`;
      return;
    }

    // Today: a "Now" line before the first flight scheduled at or after the current time
    const isToday = state.date === todayPKT();
    const nowMin  = isToday ? nowMinutesPKT() : null;
    let nowPlaced = false;
    const nowLine = () => {
      const d = document.createElement('div');
      d.className = 'now-line';
      d.id = 'now-line';
      d.innerHTML = `<span>Now · ${String(Math.floor(nowMin / 60)).padStart(2, '0')}:${String(nowMin % 60).padStart(2, '0')}</span>`;
      return d;
    };

    const frag = document.createDocumentFragment();
    shown.forEach(f => {
      if (isToday && !nowPlaced && timeToMinutes(f.st) >= nowMin) { frag.appendChild(nowLine()); nowPlaced = true; }
      frag.appendChild(flightCard(f));
    });
    if (isToday && !nowPlaced) frag.appendChild(nowLine());   // every flight is earlier than now
    resultsDiv.innerHTML = '';
    resultsDiv.appendChild(frag);
    updateNowBtn();
  }

  // "Now" button (today only): scrolls so the Now line is near the top, with about one earlier
  // flight above it. The page itself always opens at the top, so the search area is seen first.
  function goToNow() {
    const line = document.getElementById('now-line');
    if (!line) return;
    const header = document.querySelector('.site-header');
    const room = line.previousElementSibling ? 150 : 16;
    const top = Math.max(0, line.getBoundingClientRect().top + window.scrollY
              - (header ? header.offsetHeight : 0) - room);
    const reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    window.scrollTo({ top, behavior: reduce ? 'instant' : 'smooth' });
  }

  // Card (BL-048, option C): 48 px logo, the airline's name in words, then flight, times, status
  function flightCard(f) {
    const code    = airlineCode(f.flight_number);
    const airline = AIRLINE_INFO[code];
    const card    = document.createElement('a');
    card.className = 'flight-card';
    card.href      = detailUrl(f.flight_number, state.date, f.type);
    card.dataset.flight = f.flight_number;
    card.dataset.type   = f.type || '';

    const logo = document.createElement('span');
    logo.className = 'fc-logo';
    const img = document.createElement('img');
    img.src     = getLogoUrl(code);
    img.alt     = '';
    img.width   = 48;
    img.height  = 48;
    img.loading = 'lazy';
    img.onerror = () => { logo.classList.add('fc-logo--text'); logo.textContent = code; };  // keep the space, show the code
    logo.appendChild(img);

    const body = document.createElement('div');
    body.className = 'fc-body';
    body.innerHTML = `
      ${airline ? `<div class="fc-airline">${esc(airline.name)}</div>` : ''}
      <div class="fc-main">
        <strong>${esc(f.flight_number)}</strong>
        <span class="fc-route">${esc(f.type ?? '')} · ${esc(f.city ?? '—')}</span>
        ${f.nature ? `<span class="flight-nature ${esc(String(f.nature).toLowerCase())}">${esc(f.nature)}</span>` : ''}
      </div>
      <div class="fc-times">ST ${esc(f.st || '—')} · ET ${esc(f.et || '—')} ·
        <span class="status-pill ${statusClass(f.status)}">${esc(f.status || '—')}</span></div>`;

    const chevron = document.createElement('span');
    chevron.className   = 'flight-card-chevron';
    chevron.textContent = '›';
    chevron.setAttribute('aria-hidden', 'true');

    card.append(logo, body, chevron);
    return card;
  }

  function detailUrl(flightNumber, date, type) {
    // type keeps an arrival and a departure with the same number apart (BL-022)
    const typePart = type ? `&type=${encodeURIComponent(type)}` : '';
    return `flight_detail.html?flight=${encodeURIComponent(flightNumber)}&date=${encodeURIComponent(date)}${typePart}`;
  }

  function saveSearchToAddress() {
    const qs = new URLSearchParams();
    if (state.date !== todayPKT()) qs.set('date', state.date);
    if (state.q)     qs.set('q', state.q);
    if (state.type)  qs.set('type', state.type);
    if (state.route) qs.set('route', state.route);
    const query = qs.toString();
    try { window.history.replaceState(null, '', window.location.pathname + (query ? `?${query}` : '')); } catch (e) { /* ignore */ }
  }

  // Remember where the visitor was in the list, so coming back lands on the same flight
  function scrollKey() { return 'resultsScroll:' + window.location.search; }

  // Opening a flight saves it to Recent and remembers the scroll position
  resultsDiv.addEventListener('click', e => {
    const card = e.target.closest('a.flight-card');
    if (!card) return;
    try { sessionStorage.setItem(scrollKey(), String(window.scrollY)); } catch (err) { /* ignore */ }
    clearTimeout(saveTimer);
    saveToLocalHistory({ label: card.dataset.flight, date: state.date, isFlightSearch: true, type: card.dataset.type });
  });

  // ===== RECENT (saved searches) =====
  // Each entry: { label, date, isFlightSearch, userLabel, [q, type, route] }
  //   Flight opened:  label = flight number, isFlightSearch = true, type
  //   Search kept:    label = summary ("Emirates · Arrivals"), q / type / route
  //   Older entries (before BL-048) have no q/type/route; their label is parsed instead.
  // userLabel = optional custom name (e.g. "Mr Haseeb")

  // A search is saved once it has been left alone for a few seconds (not on every keystroke)
  let saveTimer = null;
  function scheduleSave(delay = 4000) {
    clearTimeout(saveTimer);
    saveTimer = setTimeout(() => {
      if (!state.q && !state.type && !state.route) return;      // the plain day list is not worth saving
      if (loadedFor !== state.date || !lastShown) return;       // nothing found: not worth saving
      const parts = [];
      if (state.q)     parts.push(state.q.trim());
      if (state.type)  parts.push(state.type + 's');
      if (state.route) parts.push(state.route);
      saveToLocalHistory({ label: parts.join(' · '), date: state.date, isFlightSearch: false,
                           q: state.q.trim(), type: state.type, route: state.route });
    }, delay);
  }

  // Read saved searches; a missing, damaged or blocked store gives an empty list
  function loadHistory() {
    try {
      const parsed = JSON.parse(localStorage.getItem('flightHistory') || '[]');
      return Array.isArray(parsed) ? parsed.filter(h => h && typeof h.label === 'string') : [];
    } catch (e) {
      return [];
    }
  }

  function saveHistory(history) {
    try {
      localStorage.setItem('flightHistory', JSON.stringify(history));
    } catch (e) {
      // storage full or blocked (e.g. private mode): searching still works
    }
  }

  function saveToLocalHistory(entry) {
    let history = loadHistory();
    const same = h => h.label.toLowerCase() === entry.label.toLowerCase() && h.date === entry.date;
    // Keep the custom name (e.g. "Mr Haseeb") when the same thing is saved again
    const existing = history.find(same);
    history = history.filter(h => !same(h));
    history.unshift({ ...entry, userLabel: existing ? (existing.userLabel || '') : '' });
    if (history.length > 10) history.length = 10;
    saveHistory(history);
    renderHistory();
  }

  function historyText(h) {
    // Older search entries already carry their date in the label ("2026-11-09 · Arrivals")
    if (!h.isFlightSearch && /^\d{4}-\d{2}-\d{2}/.test(h.label)) return h.label;
    const day = /^\d{4}-\d{2}-\d{2}$/.test(h.date || '') ? ` · ${dayName(h.date)}` : '';
    return h.label + day;
  }

  function openHistory(h) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(h.date || '')) return;
    if (h.isFlightSearch) {
      Object.assign(state, { q: h.label, type: h.type || '', route: '' });
    } else if ('q' in h || 'type' in h || 'route' in h) {
      Object.assign(state, { q: h.q || '', type: h.type || '', route: h.route || '' });
    } else {   // older entry: "2026-11-09 · Arrivals · Domestic"
      Object.assign(state, {
        q: '',
        type:  h.label.includes('· Arrivals') ? 'Arrival' : h.label.includes('· Departures') ? 'Departure' : '',
        route: h.label.includes('· International') ? 'International' : h.label.includes('· Domestic') ? 'Domestic' : '',
      });
    }
    state.date = h.date;
    syncControls();
    loadDay();
    window.scrollTo({ top: document.getElementById('search-container').offsetTop - 70, behavior: 'smooth' });
  }

  function renderHistory() {
    const history = loadHistory();
    historyList.innerHTML = '';

    if (!history.length) {
      historyList.innerHTML = '<li class="history-empty">Flights you open and searches you keep show up here.</li>';
      return;
    }

    history.forEach((h, index) => {
      const li = document.createElement('li');

      const labelWrap = document.createElement('div');
      labelWrap.className = 'history-label-wrap';

      const span = document.createElement('span');
      span.className    = 'history-flight';
      span.textContent  = historyText(h);
      span.style.cursor = 'pointer';
      span.addEventListener('click', () => openHistory(h));

      const userLabelEl = document.createElement('span');
      userLabelEl.className    = 'history-user-label';
      userLabelEl.textContent  = h.userLabel || '+ add label';
      userLabelEl.style.cursor = 'pointer';
      userLabelEl.addEventListener('click', (e) => {
        e.stopPropagation();
        startEditing(userLabelEl, index);
      });

      labelWrap.appendChild(span);
      labelWrap.appendChild(userLabelEl);

      const btnDiv = document.createElement('div');
      btnDiv.className = 'history-buttons';

      const upBtn = document.createElement('button');
      upBtn.textContent = '↑';
      upBtn.title = 'Move up';
      upBtn.addEventListener('click', () => moveHistory(index, -1));

      const downBtn = document.createElement('button');
      downBtn.textContent = '↓';
      downBtn.title = 'Move down';
      downBtn.addEventListener('click', () => moveHistory(index, 1));

      const delBtn = document.createElement('button');
      delBtn.textContent = '×';
      delBtn.title       = 'Remove';
      delBtn.className   = 'history-delete';
      delBtn.addEventListener('click', () => deleteHistory(index));

      btnDiv.appendChild(upBtn);
      btnDiv.appendChild(downBtn);
      btnDiv.appendChild(delBtn);

      li.appendChild(labelWrap);
      li.appendChild(btnDiv);
      historyList.appendChild(li);
    });
  }

  function startEditing(el, index) {
    const history = loadHistory();
    if (!history[index]) return;
    const current = history[index].userLabel || '';

    const input       = document.createElement('input');
    input.type        = 'text';
    input.value       = current;
    input.className   = 'history-label-input';
    input.placeholder = 'e.g. Mr Haseeb';
    input.maxLength   = 40;

    el.replaceWith(input);
    input.focus();

    function save() {
      history[index].userLabel = input.value.trim();
      saveHistory(history);
      renderHistory();
    }

    input.addEventListener('blur', save);
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter')  input.blur();
      if (e.key === 'Escape') { input.value = current; input.blur(); }
    });
  }

  function moveHistory(index, direction) {
    const history = loadHistory();
    const newIndex = index + direction;
    if (newIndex < 0 || newIndex >= history.length) return;
    [history[index], history[newIndex]] = [history[newIndex], history[index]];
    saveHistory(history);
    renderHistory();
  }

  function deleteHistory(index) {
    const history = loadHistory();
    history.splice(index, 1);
    saveHistory(history);
    renderHistory();
  }

  clearHistoryBtn.addEventListener('click', () => {
    try { localStorage.removeItem('flightHistory'); } catch (e) { /* ignore */ }
    renderHistory();
  });

  // ===== BACK TO TOP =====
  const floatBtn = document.createElement('button');
  floatBtn.id = 'float-nav-btn';
  floatBtn.textContent = '↑ Top';
  floatBtn.setAttribute('aria-label', 'Back to top');
  document.body.appendChild(floatBtn);
  window.addEventListener('scroll', () => floatBtn.classList.toggle('visible', window.scrollY > 300));
  floatBtn.addEventListener('click', () => window.scrollTo({ top: 0, behavior: 'smooth' }));

  // ===== NOW BUTTON (today only, while the Now line is off-screen) =====
  const nowBtn = document.createElement('button');
  nowBtn.id   = 'now-btn';
  nowBtn.type = 'button';
  document.body.appendChild(nowBtn);
  nowBtn.addEventListener('click', goToNow);

  function updateNowBtn() {
    const line = document.getElementById('now-line');
    if (!line) { nowBtn.classList.remove('visible'); return; }
    const r = line.getBoundingClientRect();
    const header = document.querySelector('.site-header');
    const below  = r.top >= window.innerHeight - 60;            // under the screen (or behind the buttons)
    const above  = r.bottom <= (header ? header.offsetHeight : 0);
    nowBtn.textContent = below ? 'Now ↓' : 'Now ↑';
    nowBtn.setAttribute('aria-label', `Go to the current time (${below ? 'down' : 'up'} the list)`);
    nowBtn.classList.toggle('visible', below || above);
    nowBtn.classList.toggle('stacked', window.scrollY > 300);   // sits above "↑ Top" when that shows
  }
  window.addEventListener('scroll', updateNowBtn, { passive: true });
  window.addEventListener('resize', updateNowBtn);

  // ===== START =====
  syncControls();
  renderHistory();
  loadDay();
  loadFreshness();
});
