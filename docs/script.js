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

  // Search click
  searchBtn.addEventListener('click', async () => {
    const query = document.getElementById('flight-search').value.trim();
    const date = dateInput.value;

    if (!query) {
  // Fetch all flights for the date
  const { data: flights, error } = await client
    .from('flights')
    .select('*')
    .eq('scheduled_date', date);
} else {
  // Existing filtered fetch
}


    try {
      const { data: flights, error } = await client
        .from('flights')
        .select('*')
        .ilike('flight_number', `%${query}%`)
        .eq('scheduled_date', date);

      if (error) throw error;

      if (!flights.length) {
        resultsDiv.innerHTML = '<p>No flights found.</p>';
        return;
      }

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
    historyList.innerHTML = history.map(h =>
      `<li onclick="loadFlight('${h.flightNumber}','${h.date}')">${h.flightNumber} | ${h.date}</li>`
    ).join('');
  }

  window.loadFlight = function(flightNumber, date) {
    document.getElementById('flight-search').value = flightNumber;
    dateInput.value = date;
    searchBtn.click();
  };

 window.viewHistory = function(flightNumber, date) {
    window.location.href = `flight_detail.html?flight=${flightNumber}&date=${date}`;
  };
