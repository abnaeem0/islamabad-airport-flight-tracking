// Setup
const supabaseUrl = 'https://YOUR_SUPABASE_URL.supabase.co';
const supabaseKey = 'YOUR_SUPABASE_ANON_KEY';
const { createClient } = supabase;
const supabase = createClient(supabaseUrl, supabaseKey);

document.getElementById('flight-date').valueAsDate = new Date();

const searchBtn = document.getElementById('search-btn');
const resultsDiv = document.getElementById('search-results');

searchBtn.addEventListener('click', async () => {
  const query = document.getElementById('flight-search').value.trim();
  const date = document.getElementById('flight-date').value;

  if (!query) return alert('Enter a flight number');

  // Fetch flights from Supabase
  const { data, error } = await supabase
    .from('flights')
    .select('*')
    .ilike('flight_number', `%${query}%`)
    .eq('flight_date', date);

  if (error) return alert('Error fetching flights');

  if (!data.length) {
    resultsDiv.innerHTML = '<p>No flights found.</p>';
    return;
  }

  // Display results
  resultsDiv.innerHTML = data.map(f => `
    <div class="flight-card">
      <strong>${f.flight_number}</strong> ${f.from_airport} → ${f.to_airport}<br>
      Scheduled: ${f.scheduled_time} | Estimated: ${f.estimated_time} | Status: ${f.status}
      <button onclick="viewHistory('${f.flight_number}', '${date}')">History</button>
    </div>
  `).join('');

  saveToLocalHistory(query, date);
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
  const list = document.getElementById('history-list');
  list.innerHTML = history.map(h => `<li onclick="loadFlight('${h.flightNumber}','${h.date}')">${h.flightNumber} | ${h.date}</li>`).join('');
}

function loadFlight(flightNumber, date) {
  document.getElementById('flight-search').value = flightNumber;
  document.getElementById('flight-date').value = date;
  searchBtn.click();
}

// Flight history toggle
async function viewHistory(flightNumber, date) {
  const { data } = await supabase
    .from('flight_snapshots')
    .select('updated_at, scheduled_time, status')
    .eq('flight_number', flightNumber)
    .eq('flight_date', date)
    .order('updated_at', { ascending: true });

  if (!data || !data.length) return alert('No history available');

  const historyHtml = data.map(h => `<div>${h.updated_at} → ${h.scheduled_time} | ${h.status}</div>`).join('');
  alert(historyHtml); // simple MVP; can replace with modal
}

// Render history on load
renderHistory();
