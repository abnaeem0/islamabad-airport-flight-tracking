document.addEventListener('DOMContentLoaded', () => {

  if (typeof supabase === 'undefined') {
    console.error('Supabase failed to load.');
    return;
  }

  const supabaseUrl = 'https://sbaweaytsmdmhaclgcwr.supabase.co';
  const supabaseKey = 'sb_publishable_PBY7Y_HM60Ijqw9j6iOGeg_XqLDI7SS';
  const client = supabase.createClient(supabaseUrl, supabaseKey);

  const searchBtn = document.getElementById('search-btn');
  const resultsDiv = document.getElementById('search-results');
  const historyList = document.getElementById('history-list');
  const dateInput = document.getElementById('flight-date');
  const citySelect = document.getElementById('city-filter');
  const clearHistoryBtn = document.getElementById('clear-history-btn');

  if (dateInput) dateInput.valueAsDate = new Date();

  // Show history on page load
  renderHistory();

  // --- Utility ---
  function timeToMinutes(t) {
    if (!t) return 0;
    const [h, m] = t.split(':').map(Number);
    return h * 60 + m;
  }

  // --- Search ---
  searchBtn.addEventListener('click', async () => {
    const query = document.getElementById('flight-search').value.trim();
    const date = dateInput.value;
    const typeFilter = document.getElementById('flight-type').value;
    const cityFilter = citySelect.value;

    if (!date) {
      resultsDiv.innerHTML = '<p class="error">Please select a date.</p>';
      return;
    }

    searchBtn.disabled = true;
    searchBtn.textContent = 'Searching...';
    resultsDiv.innerHTML = '<p>Loading...</p>';

    try {
      let queryBuilder = client.from('flights').select('*').eq('scheduled_date', date);
      if (query) queryBuilder = queryBuilder.ilike('flight_number', `%${query}%`);
      if (typeFilter) queryBuilder = queryBuilder.eq('type', typeFilter);

      const { data: flights, error } = await queryBuilder;
      if (error) throw error;

      // Always repopulate city dropdown from full results (preserving selection)
      const cities = [...new Set(flights.map(f => f.city))].sort();
      citySelect.innerHTML = '<option value="">All Cities</option>' +
        cities.map(c => `<option value="${c}" ${c === cityFilter ? 'selected' : ''}>${c}</option>`).join('');

      // Apply city filter client-side so dropdown stays intact
      const filtered = cityFilter
        ? flights.filter(f => f.city === cityFilter)
        : flights;

      if (!filtered.length) {
        resultsDiv.innerHTML = '<p>No flights found.</p>';
        return;
      }

      filtered.sort((a, b) => timeToMinutes(a.st) - timeToMinutes(b.st));

      // Render results safely without inline onclick
      resultsDiv.innerHTML = '';
      filtered.forEach(f => {
        const card = document.createElement('div');
        card.className = 'flight-card';
        card.innerHTML = `
          ${f.airline_logo ? `<img src="${f.airline_logo}" width="40" alt="${f.flight_number} logo">` : ''}
          <strong>${f.flight_number}</strong> | ${f.type} | ${f.city}<br>
          ST: ${f.st} | ET: ${f.et} | Status: ${f.status}
        `;
        const btn = document.createElement('button');
        btn.textContent = 'History';
        btn.addEventListener('click', () => viewHistory(f.flight_number, date));
        card.appendChild(btn);
        resultsDiv.appendChild(card);
      });

      if (query) saveToLocalHistory(query, date);

    } catch (err) {
      console.error(err);
      resultsDiv.innerHTML = '<p class="error">Error fetching flights. Please try again.</p>';
    } finally {
      searchBtn.disabled = false;
      searchBtn.textContent = 'Search';
    }
  });

  // --- Local History ---
  function saveToLocalHistory(flightNumber, date) {
    let history = JSON.parse(localStorage.getItem('flightHistory') || '[]');
    history = history.filter(h => !(h.flightNumber === flightNumber && h.date === date));
    history.unshift({ flightNumber, date });
    if (history.length > 5) history.pop();
    localStorage.setItem('flightHistory', JSON.stringify(history));
    renderHistory();
  }

  function renderHistory() {
    const history = JSON.parse(localStorage.getItem('flightHistory') || '[]');
    historyList.innerHTML = '';

    if (!history.length) {
      historyList.innerHTML = '<li>No recent searches.</li>';
      return;
    }

    history.forEach((h, index) => {
      const li = document.createElement('li');

      const span = document.createElement('span');
      span.textContent = `${h.flightNumber} | ${h.date}`;
      span.style.cursor = 'pointer';
      span.addEventListener('click', () => loadFlight(h.flightNumber, h.date));

      const upBtn = document.createElement('button');
      upBtn.textContent = '↑';
      upBtn.className = 'move-up';
      upBtn.addEventListener('click', () => moveHistory(index, -1));

      const downBtn = document.createElement('button');
      downBtn.textContent = '↓';
      downBtn.className = 'move-down';
      downBtn.addEventListener('click', () => moveHistory(index, 1));

      const btnDiv = document.createElement('div');
      btnDiv.className = 'history-buttons';
      btnDiv.appendChild(upBtn);
      btnDiv.appendChild(downBtn);

      li.appendChild(span);
      li.appendChild(btnDiv);
      historyList.appendChild(li);
    });
  }

  function moveHistory(index, direction) {
    const history = JSON.parse(localStorage.getItem('flightHistory') || '[]');
    const newIndex = index + direction;
    if (newIndex < 0 || newIndex >= history.length) return;
    [history[index], history[newIndex]] = [history[newIndex], history[index]];
    localStorage.setItem('flightHistory', JSON.stringify(history));
    renderHistory();
  }

  // Wire up clear history button
  clearHistoryBtn.addEventListener('click', () => {
    localStorage.removeItem('flightHistory');
    renderHistory();
  });

  // --- Navigation ---
  window.loadFlight = function(flightNumber, date) {
    document.getElementById('flight-search').value = flightNumber;
    dateInput.value = date;
    searchBtn.click();
  };

  window.viewHistory = function(flightNumber, date) {
    window.location.href = `flight_detail.html?flight=${encodeURIComponent(flightNumber)}&date=${encodeURIComponent(date)}`;
  };

});
