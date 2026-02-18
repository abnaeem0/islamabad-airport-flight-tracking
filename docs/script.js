document.addEventListener('DOMContentLoaded', () => {

  // Ensure Supabase loaded
  if (typeof supabase === 'undefined') {
    console.error('Supabase failed to load.');
    return;
  }

  const supabaseUrl = 'https://sbaweaytsmdmhaclgcwr.supabase.co';
  const supabaseKey = 'sb_publishable_PBY7Y_HM60Ijqw9j6iOGeg_XqLDI7SS';
  const client = supabase.createClient(supabaseUrl, supabaseKey);

  // DOM elements
  const searchBtn = document.getElementById('search-btn');
  const resultsDiv = document.getElementById('search-results');
  const historyList = document.getElementById('history-list');
  const dateInput = document.getElementById('flight-date');

  if (dateInput) dateInput.valueAsDate = new Date();

  searchBtn.addEventListener('click', async () => {
    const query = document.getElementById('flight-search').value.trim();
    const date = dateInput.value;
    const typeFilter = document.getElementById('flight-type').value;
    const cityFilter = document.getElementById('city-filter').value;

    try {
      // Base query
      let queryBuilder = client.from('flights').select('*').eq('scheduled_date', date);
  
      if (query) queryBuilder = queryBuilder.ilike('flight_number', `%${query}%`);
      if (typeFilter) queryBuilder = queryBuilder.eq('type', typeFilter);
      // City filter applied after type check (optional)
      if (cityFilter) queryBuilder = queryBuilder.ilike('city', `%${cityFilter}%`);
  
      const { data: flights, error } = await queryBuilder;
      if (error) throw error;
  
      if (!flights.length) {
        resultsDiv.innerHTML = '<p>No flights found.</p>';
        return;
      }
  
      // Sort by scheduled time
      flights.sort((a, b) => {
        const timeToMinutes = t => {
          const [h, m] = t.split(':').map(Number);
          return h * 60 + m;
        };
        return timeToMinutes(a.st) - timeToMinutes(b.st);
      });
  
      // Populate city filter dynamically
      const citySelect = document.getElementById('city-filter');
      const cities = [...new Set(flights.map(f => f.city))].sort();
      citySelect.innerHTML = '<option value="">All Cities</option>' + cities.map(c => `<option value="${c}">${c}</option>`).join('');
  
      // Render results
      resultsDiv.innerHTML = flights.map(f => `
        <div class="flight-card">
          ${f.airline_logo ? `<img src="${f.airline_logo}" width="40">` : ''}
          <strong>${f.flight_number}</strong> | ${f.type} | ${f.city}<br>
          ST: ${f.st} | ET: ${f.et} | Status: ${f.status}<br>
          <button onclick="viewHistory('${f.flight_number}','${date}')">History</button>
        </div>
      `).join('');
  
      saveToLocalHistory(query, date);
  
    } catch (err) {
      console.error(err);
      alert('Error fetching flights');
    }
  });

  // Local history
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
  
    history.forEach((h, index) => {
      const li = document.createElement('li');
      li.innerHTML = `
        <span onclick="loadFlight('${h.flightNumber}','${h.date}')">${h.flightNumber} | ${h.date}</span>
        <div class="history-buttons">
          <button class="move-up" data-index="${index}">↑</button>
          <button class="move-down" data-index="${index}">↓</button>
        </div>
      `;
      historyList.appendChild(li);
    });
  
    // Add button functionality
    document.querySelectorAll('.move-up').forEach(btn => {
      btn.onclick = () => moveHistory(parseInt(btn.dataset.index), -1);
    });
    document.querySelectorAll('.move-down').forEach(btn => {
      btn.onclick = () => moveHistory(parseInt(btn.dataset.index), 1);
    });
  }

  function moveHistory(index, direction) {
    const history = JSON.parse(localStorage.getItem('flightHistory') || '[]');
    const newIndex = index + direction;
    if (newIndex < 0 || newIndex >= history.length) return; // bounds check
  
    // Swap items
    [history[index], history[newIndex]] = [history[newIndex], history[index]];
  
    // Save and re-render
    localStorage.setItem('flightHistory', JSON.stringify(history));
    renderHistory();
  }



  window.loadFlight = function(flightNumber, date) {
    document.getElementById('flight-search').value = flightNumber;
    dateInput.value = date;
    searchBtn.click();
  };

 window.viewHistory = function(flightNumber, date) {
    window.location.href = `flight_detail.html?flight=${flightNumber}&date=${date}`;
  };
  
});
