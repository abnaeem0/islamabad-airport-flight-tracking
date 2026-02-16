document.addEventListener('DOMContentLoaded', async () => {
  const urlParams = new URLSearchParams(window.location.search);
  const flightNumber = urlParams.get('flight');
  const date = urlParams.get('date');

  const supabaseUrl = 'https://sbaweaytsmdmhaclgcwr.supabase.co';
  const supabaseKey = 'sb_publishable_PBY7Y_HM60Ijqw9j6iOGeg_XqLDI7SS';
  const client = supabase.createClient(supabaseUrl, supabaseKey);

  const pktTime = new Date(s.scraped_at).toLocaleString('en-GB', {
  timeZone: 'Asia/Karachi',
  year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit'
  });

  
  try {
    const { data: snapshots, error } = await client
      .from('flight_snapshots')
      .select('*')
      .eq('flight_number', flightNumber)
      .eq('scheduled_date', date)
      .order('scraped_at', { ascending: true });

    if (error) throw error;
    if (!snapshots.length) {
      document.getElementById('flight-info').textContent = 'No history available';
      return;
    }

    const tbody = document.querySelector('#snapshot-table tbody');
    snapshots.forEach(s => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${formatPKT(s.scraped_at)}</td>
        <td>${s.st || s.ST}</td>
        <td>${s.et || s.ET}</td>
        <td>${s.status}</td>
        <td>${s.flight_number}</td>
        <td>${s.type}</td>
        <td>${s.city}</td>
      `;
      tbody.appendChild(tr);
    });

  } catch (err) {
    console.error(err);
    document.getElementById('flight-info').textContent = 'Error loading history';
  }
});
