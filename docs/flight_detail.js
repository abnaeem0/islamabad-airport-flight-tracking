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

  // ===== UTILS =====

  // Format a UTC timestamp string as PKT local time
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

  // Return value or a dash for null/undefined
  function display(val) {
    return val ?? '—';
  }

  // Human-readable "X mins ago" from a UTC timestamp string
  function timeAgo(dateStr) {
    const diffMs   = Date.now() - new Date(dateStr).getTime();
    const diffMins = Math.floor(diffMs / 60000);
    if (diffMins < 1)  return 'just now';
    if (diffMins < 60) return `${diffMins} min${diffMins > 1 ? 's' : ''} ago`;
    const diffHrs = Math.floor(diffMins / 60);
    return `${diffHrs} hour${diffHrs > 1 ? 's' : ''} ago`;
  }

  // Human-readable label for each change_type value
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
  
  const lastRefreshedEl   = document.getElementById('last-refreshed');
  const tbody             = document.querySelector('#snapshot-table tbody');

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
      // Freshness is non-critical — fail silently
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

      // Populate flight header from first snapshot (only on first load)
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

    } catch (err) {
      console.error(err);
      // Only show error message on first load — don't wipe page on background refresh failure
      if (flightInfoDiv.querySelector('#flight-number') === null) {
        flightInfoDiv.innerHTML = '<p>Error loading flight history. Please try again.</p>';
      }
    }
  }

  // ===== INIT =====
  await fetchAndRender();
  await fetchFreshness();

  // Checkbox no longer needed (we only store changes now) but kept for compatibility
  //toggleAllCheckbox.addEventListener('change', fetchAndRender);

  // Auto-refresh every 5 minutes
  setInterval(async () => {
    await fetchAndRender();
    await fetchFreshness();
  }, 5 * 60 * 1000);

});
