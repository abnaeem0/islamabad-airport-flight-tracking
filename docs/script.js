document.addEventListener('DOMContentLoaded', () => {

  if (typeof supabase === 'undefined') {
    console.error('Supabase failed to load.');
    return;
  }

  const supabaseUrl = 'https://sbaweaytsmdmhaclgcwr.supabase.co';
  const supabaseKey = 'sb_publishable_PBY7Y_HM60Ijqw9j6iOGeg_XqLDI7SS';
  const client = supabase.createClient(supabaseUrl, supabaseKey);

  const searchBtn       = document.getElementById('search-btn');
  const resultsDiv      = document.getElementById('search-results');
  const historyList     = document.getElementById('history-list');
  const dateInput       = document.getElementById('flight-date');
  const citySelect      = document.getElementById('city-filter');
  const cityFilterWrap  = document.getElementById('city-filter-wrap');
  const natureSelect    = document.getElementById('nature-filter');
  const clearHistoryBtn = document.getElementById('clear-history-btn');

  if (dateInput) dateInput.valueAsDate = new Date();

  // Show history on page load
  renderHistory();

  // Floating back-to-top button — hidden until user scrolls down 300px
  const floatBtn = document.createElement('button');
  floatBtn.id = 'float-nav-btn';
  floatBtn.textContent = '↑ Top';
  floatBtn.setAttribute('aria-label', 'Back to top');
  document.body.appendChild(floatBtn);

  window.addEventListener('scroll', () => {
    floatBtn.classList.toggle('visible', window.scrollY > 300);
  });

  floatBtn.addEventListener('click', () => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  });

  // Auto-search when type or nature filter changes (city is post-search only)
  document.getElementById('flight-type').addEventListener('change', () => searchBtn.click());
  document.getElementById('nature-filter').addEventListener('change', () => searchBtn.click());

  // Auto-search when city filter changes (only relevant after first search)
  citySelect.addEventListener('change', () => searchBtn.click());

  // --- Utility ---
  function timeToMinutes(t) {
    if (!t) return 0;
    const [h, m] = t.split(':').map(Number);
    return h * 60 + m;
  }

  // Normalise flight number input — strip spaces, hyphens, underscores, uppercase
  function normaliseQuery(input) {
    return input.replace(/[\s\-_]/g, '').toUpperCase();
  }

  // --- Airline Logos ---
  const LOCAL_AIRLINES = ['PK', 'PF', '9P', 'PA'];
  const REPO_LOGOS     = 'https://abnaeem0.github.io/islamabad-airport-flight-tracking/logos/';

  function getLogoUrl(flightNumber) {
    const code = flightNumber.slice(0, 2).toUpperCase();
    if (LOCAL_AIRLINES.includes(code)) {
      return `${REPO_LOGOS}${code.toLowerCase()}.png`;
    }
    return `https://pics.avs.io/60/60/${code}.png`;
  }

  // --- Search ---
  searchBtn.addEventListener('click', async () => {
    const raw          = document.getElementById('flight-search').value.trim();
    const query        = raw ? normaliseQuery(raw) : '';
    const date         = dateInput.value;
    const typeFilter   = document.getElementById('flight-type').value;
    const cityFilter   = citySelect.value;
    const natureFilter = natureSelect ? natureSelect.value : '';

    if (!date) {
      resultsDiv.innerHTML = '<p class="error">Please select a date.</p>';
      return;
    }

    searchBtn.disabled    = true;
    searchBtn.textContent = 'Searching...';
    resultsDiv.innerHTML  = '<p>Loading...</p>';

    try {
      let queryBuilder = client.from('flights').select('*').eq('scheduled_date', date);
      if (query)        queryBuilder = queryBuilder.ilike('flight_number', `%${query}%`);
      if (typeFilter)   queryBuilder = queryBuilder.eq('type', typeFilter);
      if (natureFilter) queryBuilder = queryBuilder.eq('nature', natureFilter);

      const { data: flights, error } = await queryBuilder;
      if (error) throw error;

      // Populate city filter above results and show it
      const cities = [...new Set(flights.map(f => f.city).filter(Boolean))].sort();
      citySelect.innerHTML = '<option value="">All Cities</option>' +
        cities.map(c => `<option value="${c}" ${c === cityFilter ? 'selected' : ''}>${c}</option>`).join('');
      cityFilterWrap.style.display = cities.length ? 'flex' : 'none';

      // Apply city filter client-side so dropdown stays intact
      const filtered = cityFilter
        ? flights.filter(f => f.city === cityFilter)
        : flights;

      if (!filtered.length) {
        resultsDiv.innerHTML = '<p>No flights found.</p>';
        return;
      }

      filtered.sort((a, b) => timeToMinutes(a.st) - timeToMinutes(b.st));

      // Render results
      resultsDiv.innerHTML = '';
      filtered.forEach(f => {
        const card = document.createElement('div');
        card.className = 'flight-card';

        const img = document.createElement('img');
        img.src     = getLogoUrl(f.flight_number);
        img.alt     = f.flight_number + ' logo';
        img.width   = 40;
        img.height  = 40;
        img.onerror = function() { this.style.display = 'none'; };

        const info = document.createElement('div');
        info.innerHTML = `
          <strong>${f.flight_number}</strong> | ${f.type} | ${f.city ?? '—'}
          ${f.nature ? `<span class="flight-nature ${f.nature.toLowerCase()}">${f.nature}</span>` : ''}
          <br>ST: ${f.st ?? '—'} | ET: ${f.et ?? '—'} | Status: ${f.status ?? '—'}
        `;

        const btn = document.createElement('button');
        btn.textContent = 'History';
        btn.addEventListener('click', () => viewHistory(f.flight_number, date));

        card.appendChild(img);
        card.appendChild(info);
        card.appendChild(btn);
        resultsDiv.appendChild(card);
      });

      // Save to history
      const label = query
        ? query
        : `${date}${typeFilter ? ' · ' + typeFilter + 's' : ''}${natureFilter ? ' · ' + natureFilter : ''}`;
      saveToLocalHistory(label, date, !!query);

    } catch (err) {
      console.error(err);
      resultsDiv.innerHTML = '<p class="error">Error fetching flights. Please try again.</p>';
    } finally {
      searchBtn.disabled    = false;
      searchBtn.textContent = 'Search Flights';
    }
  });

  // --- Local History ---
  // Each entry: { label, date, isFlightSearch, userLabel }
  // label          = normalised flight number or date+type+nature string
  // isFlightSearch = true if a specific flight was searched
  // userLabel      = optional custom name (e.g. "Mr Haseeb")

  function saveToLocalHistory(label, date, isFlightSearch) {
    let history = JSON.parse(localStorage.getItem('flightHistory') || '[]');
    history = history.filter(h => !(h.label === label && h.date === date));
    history.unshift({ label, date, isFlightSearch, userLabel: '' });
    if (history.length > 10) history.pop();
    localStorage.setItem('flightHistory', JSON.stringify(history));
    renderHistory();
  }

  function saveHistory(history) {
    localStorage.setItem('flightHistory', JSON.stringify(history));
  }

  function renderHistory() {
    const history = JSON.parse(localStorage.getItem('flightHistory') || '[]');
    historyList.innerHTML = '';

    if (!history.length) {
      historyList.innerHTML = '<li class="history-empty">No recent searches.</li>';
      return;
    }

    history.forEach((h, index) => {
      const li = document.createElement('li');

      const labelWrap = document.createElement('div');
      labelWrap.className = 'history-label-wrap';

      const span = document.createElement('span');
      span.className    = 'history-flight';
      span.textContent  = h.label + (h.date && h.isFlightSearch ? ` | ${h.date}` : '');
      span.style.cursor = 'pointer';
      span.addEventListener('click', () => loadFlight(h.label, h.date, h.isFlightSearch));

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
    const history = JSON.parse(localStorage.getItem('flightHistory') || '[]');
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
    const history = JSON.parse(localStorage.getItem('flightHistory') || '[]');
    const newIndex = index + direction;
    if (newIndex < 0 || newIndex >= history.length) return;
    [history[index], history[newIndex]] = [history[newIndex], history[index]];
    saveHistory(history);
    renderHistory();
  }

  function deleteHistory(index) {
    const history = JSON.parse(localStorage.getItem('flightHistory') || '[]');
    history.splice(index, 1);
    saveHistory(history);
    renderHistory();
  }

  clearHistoryBtn.addEventListener('click', () => {
    localStorage.removeItem('flightHistory');
    renderHistory();
  });

  // --- Navigation ---
  window.loadFlight = function(label, date, isFlightSearch) {
    if (isFlightSearch) {
      document.getElementById('flight-search').value = label;
    } else {
      document.getElementById('flight-search').value = '';
      const typeSelect = document.getElementById('flight-type');
      if (label.includes('· Arrivals'))        typeSelect.value = 'Arrival';
      else if (label.includes('· Departures')) typeSelect.value = 'Departure';
      else                                      typeSelect.value = '';
      if (natureSelect) {
        if (label.includes('· International'))      natureSelect.value = 'International';
        else if (label.includes('· Domestic'))      natureSelect.value = 'Domestic';
        else                                         natureSelect.value = '';
      }
    }
    dateInput.value = date;
    searchBtn.click();
  };

  window.viewHistory = function(flightNumber, date) {
    window.location.href = `flight_detail.html?flight=${encodeURIComponent(flightNumber)}&date=${encodeURIComponent(date)}`;
  };

});
