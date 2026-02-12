// Get Supabase client from window
const client = window.supabaseClient;

// DOM elements
const searchBtn = document.getElementById('search-btn');
const resultsDiv = document.getElementById('search-results');
const historyList = document.getElementById('history-list');

// Default date = today
document.getElementById('flight-date').valueAsDate = new Date();

// Event: search button click
searchBtn.addEventListener('click', async () => {
  const query = document.getElementById('flight-search').value.trim();
  const date = document.getElementById('flight-date').value;

  if (!query) return alert('Enter a flight number');

  // Fetch flights from flights table
  const { data: flights, error } = await client
    .from('flights')
    .select('*')
    .ilike('flight_number', `%${query}%`)
    .eq('scheduled_date', date);

  if (error) {
    console.error(error);
    return alert('Error fetching flights');
  }

  if (!flights.length) {
    resultsDiv.innerHTML = '<p>No flights found.</p>';
    return;
  }

  // Render flights
  resultsDiv.innerHTML = flights.map(f => `
    <div class="flight-card">
      ${f.airline_logo ? `<img src="${f.airline_logo}" alt="logo" width="40">` : ''}
      <strong>${f.flight_number}</strong> | ${f.type} | ${f.city}<br>
      ST: ${f.ST} | ET: ${f.ET} | Status: ${f.status}<br>
      <button onclick="viewHistory('${f.flight_number}','${date}')">History</button>
    </div>
  `).join('');

  saveToLocalHistory(query, date);
});

// -------- Local history ----------
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

function loadFlight(flightNumber, date) {
  document.getElementById('flight-search').value = flightNumber;
  document.getElementById('flight-date').value = date;
  searchBtn.click();
}

// -------- Flight history ----------
async function viewHistory(flightNumber, date) {
  const { data: snapshots, error } = await client
    .from('flight_snapshots')
    .select('last_checked, ST, ET, status')
    .eq('flight_number', flightNumber)
    .eq('scheduled_date', date)
    .order('last_checked', { ascending: true });

  if (error) {
    console.error(error);
    return alert('Error fetching history');
  }

  if (!snapshots.length) return alert('No history available');

  const timeline = snapshots.map(s => `${s.last_checked} → ST: ${s.ST} | ET: ${s.ET} | ${s.status}`).join('<br>');
  alert(timeline); // Simple MVP; can replace with table/modal later
}

// Initialize local history on load
renderHistory();
