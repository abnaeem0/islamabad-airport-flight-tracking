document.addEventListener('DOMContentLoaded', async () => {

  // ===== URL PARAM VALIDATION =====
  const urlParams = new URLSearchParams(window.location.search);
  const flightNumber = urlParams.get('flight');
  const date = urlParams.get('date');
  const flightInfoDiv = document.getElementById('flight-info');

  if (!flightNumber || !date) {
    flightInfoDiv.innerHTML = '<p>Invalid page link. No flight or date specified. <a href="index.html">Go back to search</a>.</p>';
    document.getElementById('snapshot-controls').style.display = 'none';
    document.getElementById('snapshot-table').style.display = 'none';
    return;
  }

  // ===== SUPABASE =====
  const supabaseUrl = 'https://sbaweaytsmdmhaclgcwr.supabase.co';
  const supabaseKey = 'sb_publishable_PBY7Y_HM60Ijqw9j6iOGeg_XqLDI7SS';
  const client = supabase.createClient(supabaseUrl, supabaseKey);

  // ===== UTILS =====
  function formatPKT(dateStr) {
    const utcDate = new Date(dateStr + 'Z');
    return utcDate.toLocaleString('en-GB', {
      timeZone: 'Asia/Karachi',
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit'
    });
  }

  function display(val) {
    return val ?? '—';
  }

  // ===== RENDER =====
  const toggleAllCheckbox = document.getElementById('toggle-all');
  const tbody = document.querySelector('#snapshot-table tbody');
  const lastRefreshedEl = document.getElementById('last-refreshed');

  function renderSnapshots(snapshots) {
    tbody.innerHTML = '';
    let prev = null;

    snapshots.forEach(s => {
      const st = s.st;
      const et = s.et;
      const status = s.status;

      const showRow = toggleAllCheckbox.checked
        || !prev
        || st !== prev.st
        || et !== prev.et
        || status !== prev.status;

      if (showRow) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${formatPKT(s.scraped_at)}</td>
          <td>${display(st)}</td>
          <td>${display(et)}</td>
          <td>${display(status)}</td>
        `;
        tbody.appendChild(tr);
        prev = { st, et, status };
      }
    });

    if (!tbody.hasChildNodes()) {
      tbody.innerHTML = '<tr><td colspan="4">No snapshot data to display.</td></tr>';
    }
  }

  // ===== FETCH =====
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

      // Populate header from first snapshot (only on first load)
      if (flightInfoDiv.querySelector('#flight-number') === null) {
        const first = snapshots[0];
        flightInfoDiv.innerHTML = `
          <p id="flight-number">Flight: ${display(first.flight_number)}</p>
          <p id="flight-date">Date: ${display(first.scheduled_date)}</p>
          <p id="flight-type">Type: ${display(first.type)}</p>
          <p id="flight-city">${first.type === 'Arrival' ? 'From' : 'To'}: ${display(first.city)}</p>
        `;
      }

      renderSnapshots(snapshots);

      // Update last refreshed timestamp
      const now = new Date().toLocaleString('en-GB', {
        timeZone: 'Asia/Karachi',
        hour: '2-digit', minute: '2-digit', second: '2-digit'
      });
      if (lastRefreshedEl) lastRefreshedEl.textContent = `Last refreshed: ${now}`;

    } catch (err) {
      console.error(err);
      // Only show error on first load — don't wipe the page on a background refresh failure
      if (flightInfoDiv.querySelector('#flight-number') === null) {
        flightInfoDiv.innerHTML = '<p>Error loading flight history. Please try again.</p>';
      }
    }
  }

  // Initial load
  await fetchAndRender();

  // Wire up checkbox after first load
  toggleAllCheckbox.addEventListener('change', fetchAndRender);

  // Auto-refresh every 5 minutes
  setInterval(fetchAndRender, 5 * 60 * 1000);

});
