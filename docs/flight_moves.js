// BL-050: flights the airport board moved to the next day's list after a delay past midnight.
//
// Reads isb_flight_moves_view (rule and tests: sql/isb_flight_moves_view.sql). Used by the search
// page (one line on the card) and the details page (one notice). Nothing here changes data. If
// the view is missing, slow or errors, every function returns "no move" and both pages show
// exactly what they showed before.
//
// A move links two rows of the same flight number and direction:
//   from  the scheduled day (D), which the board dropped
//   to    the next day (D+1), where the board now lists the flight at its delayed time
// kind 'took_over' means the D+1 row first belonged to that day's own flight of the same number;
// we say so plainly and do not guess whether that flight was cancelled.

window.FlightMoves = (() => {
  const VIEW       = 'isb_flight_moves_view';
  const COLUMNS    = 'flight_number,type,from_date,from_st,to_date,moved_time,kind,to_first_st';
  const TIMEOUT_MS = 4000;                       // never hold a page up for this extra line
  const DATE_RE    = /^\d{4}-\d{2}-\d{2}$/;

  function esc(val) {
    return String(val ?? '').replace(/[&<>"']/g, c =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  // "2026-11-02" -> "2 Nov"
  function shortDate(iso) {
    const d = new Date(`${iso}T00:00:00Z`);
    if (isNaN(d)) return esc(iso);
    return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' });
  }

  // Every move that touches one list date (moved away from it, or onto it). Always resolves.
  async function forDate(client, date) {
    if (!client || !DATE_RE.test(date || '')) return [];
    const read = client.from(VIEW).select(COLUMNS).or(`from_date.eq.${date},to_date.eq.${date}`)
      .then(({ data, error }) => (error || !Array.isArray(data) ? [] : data), () => []);
    const timeout = new Promise(resolve => setTimeout(() => resolve([]), TIMEOUT_MS));
    try { return await Promise.race([read, timeout]); } catch (e) { return []; }
  }

  // The move for one row on one date, or null. side 'from' = this row was moved away;
  // side 'to' = this row now shows a flight moved here from the day before.
  function find(moves, flightNumber, type, date) {
    for (const m of moves || []) {
      if (m.flight_number !== flightNumber || (type && m.type !== type)) continue;
      if (m.from_date === date) return { side: 'from', move: m };
      if (m.to_date === date)   return { side: 'to', move: m };
    }
    return null;
  }

  // One short line for a search card (HTML, escaped)
  function cardLine(link) {
    if (!link) return '';
    const m = link.move;
    if (link.side === 'from') {
      return `Delayed past midnight · now listed on ${shortDate(m.to_date)} at ${esc(m.moved_time)}`;
    }
    if (m.kind === 'took_over') {
      return `Now shows the delayed ${shortDate(m.from_date)} ${esc(m.from_st)} flight · ` +
             `a separate ${esc(m.to_first_st)} flight isn’t listed`;
    }
    return `The ${shortDate(m.from_date)} ${esc(m.from_st)} flight, delayed to ${esc(m.moved_time)}`;
  }

  // Status pill text for a search card: the board says "Dropped", but the flight was moved
  function cardStatus(link, status) {
    return link && link.side === 'from' && /dropped/i.test(status || '') ? 'Moved to next day' : status;
  }

  // Notice for the details page. otherRow = the other day's current row (or null), for its
  // latest status. Returns { html, date } where date is the other day to link to.
  function detailNotice(link, otherRow) {
    if (!link) return null;
    const m = link.move;
    const latest = otherRow && otherRow.status
      ? ` Latest there: ${esc(otherRow.status)}${otherRow.et ? ` ${esc(otherRow.et)}` : ''}.` : '';
    if (link.side === 'from') {
      return {
        date: m.to_date,
        html: `<strong>Moved to the next day.</strong> After a delay past midnight, the airport ` +
              `board took this flight off ${shortDate(m.from_date)}’s list and now lists it on ` +
              `${shortDate(m.to_date)} at ${esc(m.moved_time)}.${latest}`,
        linkText: `See it on ${shortDate(m.to_date)}`,
      };
    }
    if (m.kind === 'took_over') {
      return {
        date: m.from_date,
        html: `<strong>This listing shows a delayed flight from ${shortDate(m.from_date)}.</strong> ` +
              `The board first listed ${esc(m.flight_number)} on ${shortDate(m.to_date)} at ` +
              `${esc(m.to_first_st)}, then used this listing for the ${shortDate(m.from_date)} ` +
              `${esc(m.from_st)} flight, delayed to ${esc(m.moved_time)}. A separate ` +
              `${esc(m.to_first_st)} flight isn’t listed. If your ticket is for ` +
              `${shortDate(m.to_date)} at ${esc(m.to_first_st)}, check with the airline.`,
        linkText: `See ${shortDate(m.from_date)}`,
      };
    }
    return {
      date: m.from_date,
      html: `<strong>Delayed from ${shortDate(m.from_date)}.</strong> This is the ` +
            `${shortDate(m.from_date)} flight scheduled for ${esc(m.from_st)}. After a delay past ` +
            `midnight the airport board lists it on ${shortDate(m.to_date)} at ${esc(m.moved_time)}.`,
      linkText: `See ${shortDate(m.from_date)}`,
    };
  }

  return { forDate, find, cardLine, cardStatus, detailNotice };
})();
