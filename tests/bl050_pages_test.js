// BL-050 page checks: serves docs/ with a stubbed Supabase client and checks both pages.
// usage (needs Playwright with Chromium): node tests/bl050_pages_test.js docs
const { chromium } = require('playwright');
const http = require('http'), fs = require('fs'), path = require('path');
const DOCS = path.resolve(process.argv[2]);

const STUB = `
window.supabase = { createClient() { return {
  from(table) {
    const f = { eq: [], or: null, lim: null, single: false };
    const b = {
      select() { return b; }, order() { return b; },
      eq(c, v) { f.eq.push([c, v]); return b; },
      or(s) { f.or = s; return b; },
      limit(n) { f.lim = n; return b; },
      single() { f.single = true; return b; }, maybeSingle() { f.single = true; return b; },
      then(res, rej) {
        const D = window.__DATA || {};
        if ((window.__MISSING || []).includes(table)) return Promise.resolve({ data: null, error: { message: 'missing ' + table } }).then(res, rej);
        if ((window.__HANG || []).includes(table)) return new Promise(() => {});
        let rows = (D[table] || []).filter(r => f.eq.every(([c, v]) => String(r[c]) === String(v)));
        if (f.or) { const parts = f.or.split(',').map(p => p.split('.')); rows = rows.filter(r => parts.some(([c, op, v]) => op === 'eq' && String(r[c]) === v)); }
        if (f.lim) rows = rows.slice(0, f.lim);
        const out = f.single ? (rows.length ? { data: rows[0], error: null } : { data: null, error: { message: 'none' } }) : { data: rows, error: null };
        return Promise.resolve(out).then(res, rej);
      } };
    return b;
  } }; } };`;

const HOSTILE = '<img src=x onerror="window.__pwned=1">';
const flights = [
  { flight_number: 'PK841', scheduled_date: '2026-11-01', type: 'Departure', city: 'Jeddah', status: 'Dropped', st: null, et: null, nature: 'International' },
  { flight_number: 'PA270', scheduled_date: '2026-11-01', type: 'Departure', city: 'Dubai', status: 'Departed', st: '23:30', et: '00:00', nature: 'International' },
  { flight_number: 'PK841', scheduled_date: '2026-11-02', type: 'Departure', city: 'Jeddah', status: 'Departed', st: '03:10', et: '03:30', nature: 'International' },
  { flight_number: 'PA210', scheduled_date: '2026-10-07', type: 'Departure', city: 'Dubai', status: 'Departed', st: '04:15', et: '04:27', nature: 'International' },
  { flight_number: 'XX9', scheduled_date: '2026-11-01', type: 'Arrival', city: HOSTILE, status: 'Dropped', st: null, et: null, nature: 'Domestic' },
];
const moves = [
  { flight_number: 'PK841', type: 'Departure', from_date: '2026-11-01', from_st: '21:55', to_date: '2026-11-02', moved_time: '03:10', kind: 'new_row', to_first_st: '03:10' },
  { flight_number: 'PA210', type: 'Departure', from_date: '2026-10-06', from_st: '23:55', to_date: '2026-10-07', moved_time: '04:15', kind: 'took_over', to_first_st: '23:55' },
  { flight_number: 'XX9', type: 'Arrival', from_date: '2026-11-01', from_st: HOSTILE, to_date: '2026-11-02', moved_time: '<b>03:00</b>', kind: 'new_row', to_first_st: HOSTILE },
];
const snaps = flights.map((f, i) => ({ id: i + 1, ...f, data_source: 'paa', scraped_at: '2026-11-01T10:00:00+00:00', change_type: 'status_change' }));
const DATA = { isb_flights_view: flights, isb_snapshots_view: snaps, isb_flight_moves_view: moves,
  origin_scraper_status: [{ scraper_id: 'paa_origin', last_run: new Date().toISOString() }] };

const server = http.createServer((req, res) => {
  const p = path.join(DOCS, decodeURIComponent(req.url.split('?')[0]).replace(/^\/+/, '') || 'index.html');
  if (!p.startsWith(DOCS) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404); return res.end(); }
  const type = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png' }[path.extname(p)] || 'application/octet-stream';
  res.writeHead(200, { 'Content-Type': type }); fs.createReadStream(p).pipe(res);
}).listen(0);
const base = `http://127.0.0.1:${server.address().port}/`;

let pass = 0, fail = 0;
const check = (name, ok, extra = '') => { (ok ? pass++ : fail++); console.log(`${ok ? 'PASS' : 'FAIL'} ${name}${ok ? '' : '  ' + extra}`); };

(async () => {
  const browser = await chromium.launch();
  async function open(url, { missing = [], hang = [], width = 1280 } = {}) {
    const page = await browser.newPage({ viewport: { width, height: 900 } });
    const errors = [];
    page.on('pageerror', e => errors.push(String(e)));
    await page.route('**/*', r => {
      const u = r.request().url();
      if (u.includes('supabase-js')) return r.fulfill({ contentType: 'text/javascript', body: STUB });
      if (u.startsWith(base)) return r.continue();
      return r.abort();
    });
    await page.addInitScript(([d, m, h]) => { window.__DATA = d; window.__MISSING = m; window.__HANG = h; }, [DATA, missing, hang]);
    await page.goto(base + url);
    return { page, errors };
  }
  const text = (page, sel) => page.$$eval(sel, els => els.map(e => e.textContent.replace(/\s+/g, ' ').trim()));
  const noSideScroll = page => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth);

  // --- search page ---
  {
    const { page, errors } = await open('index.html?date=2026-11-01');
    await page.waitForSelector('.flight-card');
    const cards = await page.$$eval('.flight-card', els => els.map(e => ({ f: e.dataset.flight, t: e.textContent.replace(/\s+/g, ' ') })));
    const pk = cards.find(c => c.f === 'PK841'), pa = cards.find(c => c.f === 'PA270');
    check('search 1 Nov: PK841 says moved, with new date and time', pk && pk.t.includes('Moved to next day') && pk.t.includes('Delayed past midnight · now listed on 2 Nov at 03:10'), pk && pk.t);
    check('search 1 Nov: PK841 no longer says Dropped', pk && !pk.t.includes('Dropped'), pk && pk.t);
    check('search 1 Nov: ordinary late flight PA270 has no extra line', pa && !/Delayed past midnight|Now shows/.test(pa.t), pa && pa.t);
    check('search: hostile text is shown as text, not run', !(await page.evaluate(() => window.__pwned)) && (await page.$$('.flight-card img[src="x"]')).length === 0);
    check('search 1 Nov: no script errors', errors.length === 0, errors.join(' | '));
    await page.close();
  }
  {
    const { page } = await open('index.html?date=2026-11-02');
    await page.waitForSelector('.flight-card');
    const t = (await text(page, '.flight-card[data-flight="PK841"]'))[0] || '';
    check('search 2 Nov: PK841 says it is the 1 Nov 21:55 flight', t.includes('The 1 Nov 21:55 flight, delayed to 03:10'), t);
    await page.close();
  }
  {
    const { page } = await open('index.html?date=2026-10-07');
    await page.waitForSelector('.flight-card');
    const t = (await text(page, '.flight-card[data-flight="PA210"]'))[0] || '';
    check('search 7 Oct: PA210 takeover is explained', t.includes('Now shows the delayed 6 Oct 23:55 flight · a separate 23:55 flight isn’t listed'), t);
    await page.close();
  }
  {
    const { page, errors } = await open('index.html?date=2026-11-01', { missing: ['isb_flight_moves_view'] });
    await page.waitForSelector('.flight-card');
    const t = (await text(page, '.flight-card[data-flight="PK841"]'))[0] || '';
    check('view missing: search looks as before (Dropped, no line)', t.includes('Dropped') && !t.includes('midnight') && (await page.$$('.fc-moved')).length === 0, t);
    check('view missing: no script errors', errors.length === 0, errors.join(' | '));
    await page.close();
  }
  {
    const t0 = Date.now();
    const { page } = await open('index.html?date=2026-11-01', { hang: ['isb_flight_moves_view'] });
    await page.waitForSelector('.flight-card', { timeout: 10000 });
    const ms = Date.now() - t0;
    check('view hangs: search still shows flights within ~4 s', ms < 7000, ms + ' ms');
    await page.close();
  }

  // --- details page ---
  {
    const { page, errors } = await open('flight_detail.html?flight=PK841&date=2026-11-01&type=Departure');
    await page.waitForSelector('#move-notice:not([hidden])', { timeout: 8000 }).catch(() => {});
    const t = (await text(page, '#move-notice'))[0] || '';
    const href = await page.$eval('#move-notice a', a => a.getAttribute('href')).catch(() => '');
    check('details 1 Nov: moved notice with latest status', t.includes('Moved to the next day.') && t.includes('now lists it on 2 Nov at 03:10') && t.includes('Latest there: Departed 03:30'), t);
    check('details 1 Nov: link goes to the 2 Nov departure', href === 'flight_detail.html?flight=PK841&date=2026-11-02&type=Departure', href);
    check('details 1 Nov: no script errors', errors.length === 0, errors.join(' | '));
    await page.close();
  }
  {
    const { page } = await open('flight_detail.html?flight=PK841&date=2026-11-02&type=Departure');
    await page.waitForSelector('#move-notice:not([hidden])', { timeout: 8000 }).catch(() => {});
    const t = (await text(page, '#move-notice'))[0] || '';
    check('details 2 Nov: says it is the delayed 1 Nov flight, links back', t.includes('Delayed from 1 Nov.') && t.includes('scheduled for 21:55') && t.includes('See 1 Nov'), t);
    await page.close();
  }
  {
    const { page } = await open('flight_detail.html?flight=PA210&date=2026-10-07&type=Departure');
    await page.waitForSelector('#move-notice:not([hidden])', { timeout: 8000 }).catch(() => {});
    const t = (await text(page, '#move-notice'))[0] || '';
    check('details 7 Oct takeover: explains and says check with airline', t.includes('shows a delayed flight from 6 Oct') && t.includes('A separate 23:55 flight isn’t listed') && t.includes('check with the airline'), t);
    await page.close();
  }
  {
    const { page, errors } = await open('flight_detail.html?flight=PK841&date=2026-11-01&type=Departure', { missing: ['isb_flight_moves_view'] });
    await page.waitForSelector('#flight-number', { timeout: 8000 });
    await page.waitForTimeout(500);
    check('details, view missing: header shows, notice hidden, no errors',
      await page.$eval('#move-notice', e => e.hidden) && errors.length === 0, errors.join(' | '));
    await page.close();
  }
  {
    const { page } = await open('flight_detail.html?flight=PA270&date=2026-11-01&type=Departure');
    await page.waitForSelector('#flight-number', { timeout: 8000 });
    await page.waitForTimeout(500);
    check('details, ordinary flight: no notice', await page.$eval('#move-notice', e => e.hidden));
    await page.close();
  }
  {
    const { page } = await open('flight_detail.html?flight=XX9&date=2026-11-01&type=Arrival');
    await page.waitForSelector('#move-notice:not([hidden])', { timeout: 8000 }).catch(() => {});
    check('details: hostile text in the view is escaped',
      !(await page.evaluate(() => window.__pwned)) && (await page.$$('#move-notice img, #move-notice b')).length === 0);
    await page.close();
  }

  // --- widths ---
  for (const w of [320, 375, 768, 1280]) {
    const s = await open('index.html?date=2026-11-01', { width: w });
    await s.page.waitForSelector('.fc-moved');
    const d = await open('flight_detail.html?flight=PA210&date=2026-10-07&type=Departure', { width: w });
    await d.page.waitForSelector('#move-notice:not([hidden])', { timeout: 8000 }).catch(() => {});
    check(`no sideways scroll at ${w}px (search + details)`, await noSideScroll(s.page) && await noSideScroll(d.page));
    if (w === 375) {
      await s.page.screenshot({ path: path.join(require("os").tmpdir(), "bl050_search_375.png"), fullPage: false });
      await d.page.screenshot({ path: path.join(require("os").tmpdir(), "bl050_detail_375.png"), fullPage: false });
    }
    await s.page.close(); await d.page.close();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  await browser.close(); server.close();
  process.exit(fail ? 1 : 0);
})();
