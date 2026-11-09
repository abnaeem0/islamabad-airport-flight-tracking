// Shared airline list for the search page (script.js) and the details page (flight_detail.js).
// Loaded before both scripts; exposes window.Airlines. No build step.
(function () {
  // Keyed by the two-character IATA code at the start of the flight number.
  // fsCode: the code FlightStats uses in its tracker links (null = FlightStats has no data). Usually the IATA code; where
  //   FlightStats needs something else (ICAO code or a trailing *), it is set here.
  //   Table from branch airline-links-logos, checked against live flightstats.com tracker
  //   URLs on 2026-11-04. An airline missing here falls back to its IATA code.
  // statusUrl / contact: the airline's own flight-status page and Pakistan helpline, only where
  //   checked. AirSial has no working status page, so it points at manage-reservation (PNR).
  const AIRLINE_INFO = {
    // Pakistani airlines
    PK:   { name: 'PIA',               fsCode: 'PK',
            statusUrl: 'https://book-pia.crane.aero/ibe/flightStatus?&tab=route',
            contact: '111-786-786 (from abroad +92-21-111-786-786)' },
    PA:   { name: 'Airblue',           fsCode: 'ABQ',
            statusUrl: 'https://airblue.com/flightinfo/status',
            contact: '111-247-258 (from abroad +92-51-111-247-258)' },
    '9P': { name: 'Fly Jinnah',        fsCode: 'FJL',
            statusUrl: 'https://www.flyjinnah.com/en/manage/flight-status/check-flight-status',
            contact: '021-111-000-035' },
    PF:   { name: 'AirSial',           fsCode: 'PF*',
            statusUrl: 'https://www.airsial.com/manage-reservation',
            contact: '021-38732063' },
    ER:   { name: 'SereneAir',         fsCode: 'SEP' },
    // South Air: no status page; call centre from southair.com.pk. FlightStats finds it under
    // neither Z8 nor SQB (owner checked 2026-11-09), so fsCode null = no FlightStats link. BL-047.
    Z8:   { name: 'South Air',         fsCode: null, contact: '051-111-222-999' },
    // Foreign airlines that serve Islamabad
    EK:   { name: 'Emirates',          fsCode: 'EK', statusUrl: 'https://www.emirates.com/flightstatus' },
    EY:   { name: 'Etihad',            fsCode: 'EY', statusUrl: 'https://www.etihad.com/en/manage/flight-status' },
    QR:   { name: 'Qatar Airways',     fsCode: 'QR' },
    FZ:   { name: 'flydubai',          fsCode: 'FZ' },
    G9:   { name: 'Air Arabia',        fsCode: 'G9' },
    '3L': { name: 'Air Arabia Abu Dhabi', fsCode: '3L*' },
    SV:   { name: 'Saudia',            fsCode: 'SV' },
    RX:   { name: 'Riyadh Air',        fsCode: 'RX' },   // added 2026-11-09; FlightStats code not yet checked
    XY:   { name: 'flynas',            fsCode: 'XY' },
    F3:   { name: 'flyadeal',          fsCode: 'FAD' },
    GF:   { name: 'Gulf Air',          fsCode: 'GF' },
    WY:   { name: 'Oman Air',          fsCode: 'WY' },
    OV:   { name: 'SalamAir',          fsCode: 'OMS' },
    KU:   { name: 'Kuwait Airways',    fsCode: 'KU' },
    J9:   { name: 'Jazeera Airways',   fsCode: 'J9' },
    TK:   { name: 'Turkish Airlines',  fsCode: 'TK' },
    BA:   { name: 'British Airways',   fsCode: 'BA' },
    CA:   { name: 'Air China',         fsCode: 'CA' },
    CZ:   { name: 'China Southern',    fsCode: 'CZ' },
    TG:   { name: 'Thai Airways',      fsCode: 'TG' },
    ET:   { name: 'Ethiopian',         fsCode: 'ET' },
    MS:   { name: 'EgyptAir',          fsCode: 'MS' },
    HY:   { name: 'Uzbekistan Airways', fsCode: 'HY' },
    J2:   { name: 'Azerbaijan Airlines', fsCode: 'J2' },
    KA:   { name: 'Aero Nomad',        fsCode: 'ANK' },
    SZ:   { name: 'Somon Air',         fsCode: 'SMR' },
    FG:   { name: 'Ariana Afghan',     fsCode: 'FG' },
    RQ:   { name: 'Kam Air',           fsCode: 'RQ' },
    IA:   { name: 'Iraqi Airways',     fsCode: 'IA' },
    B9:   { name: 'Iran Airtour',      fsCode: 'B9' },
    W5:   { name: 'Mahan Air',         fsCode: 'W5' },
  };

  // Flight numbers from the airport feed start with the 2-character IATA code (two letters,
  // or a letter and a digit either way round: PK301, J2143, 9P734, 3L123), then 1-4 digits
  // and maybe a letter. Anything else (e.g. an aircraft registration like APESL) gets no digits.
  function splitFlightNumber(flightNum) {
    const clean = String(flightNum || '').replace(/[^A-Za-z0-9]/g, '').toUpperCase();
    const m = clean.match(/^([A-Z0-9]{2})(\d{1,4})[A-Z]?$/);
    return m && !/^\d{2}$/.test(m[1]) ? { code: m[1], digits: m[2] } : { code: clean.slice(0, 2), digits: '' };
  }

  function getAirlineInfo(flightNum) {
    return AIRLINE_INFO[splitFlightNumber(flightNum).code] || { name: 'Airline' };
  }


  // Extra words people type when searching for an airline (lowercase; the name and the code
  // are always searchable too).
  const ALIASES = {
    PK: ['pia', 'pakistan international', 'pakistan airlines'],
    PA: ['air blue'],
    '9P': ['flyjinnah', 'jinnah'],
    PF: ['air sial', 'sial'],
    Z8: ['southair'],
    EK: ['emirate'],
    EY: ['etihad airways'],
    FZ: ['fly dubai'],
    SV: ['saudi', 'saudi arabian'],
    XY: ['fly nas'],
    F3: ['fly adeal'],
    TK: ['turkish', 'thy'],
    OV: ['salam air', 'salam'],
    J2: ['azal', 'azerbaijan'],
    HY: ['uzbekistan'],
    FG: ['ariana'],
    RQ: ['kam'],
    BA: ['british'],
    QR: ['qatar'],
    G9: ['airarabia'],
    '3L': ['airarabia'],
    J9: ['jazeera'],
    WY: ['oman'],
    KU: ['kuwait'],
    RX: ['riyadh'],
  };

  const norm = s => String(s || '').toLowerCase().replace(/[^a-z0-9]/g, '');

  // Text a search query is matched against for an airline code: code, name and aliases
  function airlineSearchText(code) {
    const info = AIRLINE_INFO[code];
    return [code, info ? info.name : '', ...(ALIASES[code] || [])].map(norm).join(' ');
  }

  window.Airlines = { AIRLINE_INFO, splitFlightNumber, getAirlineInfo, airlineSearchText, norm };
})();
