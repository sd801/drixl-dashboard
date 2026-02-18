const crypto = require('crypto');
const cookie = require('cookie');
const fs = require('fs');
const path = require('path');

const SESSION_SECRET = process.env.SESSION_SECRET || 'drixl-default-secret-change-me';
const COOKIE_NAME = 'drixl_session';
const SESSION_DAYS = 30;

function verifyToken(token) {
  if (!token) return false;
  const parts = token.split(':');
  if (parts.length !== 3) return false;
  const [status, expires, sig] = parts;
  const payload = `${status}:${expires}`;
  const expected = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('hex');
  if (sig !== expected) return false;
  if (Date.now() > parseInt(expires)) return false;
  return true;
}

function getLoginPage(error) {
  return `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Drixl Transporte – Login</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background: #0a1018; color: #c8d2dc; font-family: 'DM Sans', sans-serif;
    min-height: 100vh; display: flex; align-items: center; justify-content: center; }
  .login-wrap { width: 100%; max-width: 380px; padding: 20px; }
  .logo { text-align: center; margin-bottom: 32px; }
  .logo-sub { font-size: 11px; font-weight: 600; letter-spacing: 0.12em; text-transform: uppercase; color: #2a6f4e; margin-bottom: 4px; font-family: 'JetBrains Mono', monospace; }
  .logo h1 { font-size: 28px; font-weight: 700; color: #e8edf2; }
  .card { background: #111820; border: 1px solid #1a2332; border-radius: 12px; padding: 28px 24px; }
  .card label { display: block; font-size: 11px; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase; color: #5a6a7a; margin-bottom: 6px; font-family: 'JetBrains Mono', monospace; }
  .card input { width: 100%; padding: 10px 14px; background: #0a1018; border: 1px solid #1a2332; border-radius: 6px; color: #e8edf2; font-size: 14px; font-family: 'DM Sans', sans-serif; outline: none; margin-bottom: 16px; }
  .card input:focus { border-color: #2a6f4e; }
  .card button { width: 100%; padding: 12px; border: none; border-radius: 8px; background: linear-gradient(135deg, #2a6f4e, #4ecdc4); color: #0a1018; font-size: 14px; font-weight: 700; cursor: pointer; font-family: 'JetBrains Mono', monospace; }
  .card button:hover { box-shadow: 0 4px 20px rgba(78,205,196,0.3); }
  .error { color: #ff6b6b; font-size: 12px; text-align: center; margin-bottom: 12px; font-family: 'JetBrains Mono', monospace; }
  .footer { text-align: center; margin-top: 24px; color: #2a3a4a; font-size: 11px; }
</style>
</head>
<body>
<div class="login-wrap">
  <div class="logo">
    <div class="logo-sub">Drixl Transporte GmbH</div>
    <h1>Finance Dashboard</h1>
  </div>
  <div class="card">
    ${error ? '<div class="error">' + error + '</div>' : ''}
    <form method="POST" action="/api/middleware">
      <label for="pw">Passwort</label>
      <input type="password" id="pw" name="password" placeholder="Passwort eingeben" autofocus>
      <button type="submit">Anmelden</button>
    </form>
  </div>
  <div class="footer">Geschützter Bereich · Nur für autorisierte Benutzer</div>
</div>
</body>
</html>`;
}

// Route-to-file mapping: reads HTML from pages/ folder
const ROUTES = {
  '/': 'drixl-dashboard.html',
  '/index.html': 'drixl-dashboard.html',
  '/cashflow': 'drixl-cashflow.html',
  '/vergleich': 'drixl-vergleich.html',
  '/erfolgsrechnung': 'drixl-erfolgsrechnung.html'
};

// Cache pages in memory after first read
const PAGE_CACHE = {};

function loadPage(route) {
  if (PAGE_CACHE[route]) return PAGE_CACHE[route];
  const filename = ROUTES[route];
  if (!filename) return null;
  try {
    const filePath = path.join(__dirname, '..', 'pages', filename);
    const content = fs.readFileSync(filePath, 'utf8');
    PAGE_CACHE[route] = content;
    return content;
  } catch (e) {
    console.error('Failed to load page for route ' + route + ':', e.message);
    return null;
  }
}

module.exports = async (req, res) => {
  const cookies = cookie.parse(req.headers.cookie || '');
  const token = cookies[COOKIE_NAME];
  const isAuth = verifyToken(token);

  let reqPath = req.url.split('?')[0];
  if (reqPath === '' || reqPath === '/') reqPath = '/';

  // Handle POST login
  if (req.method === 'POST') {
    let body = '';
    await new Promise((resolve) => {
      req.on('data', chunk => body += chunk);
      req.on('end', resolve);
    });
    const params = new URLSearchParams(body);
    const password = params.get('password');
    const correctPassword = process.env.DASHBOARD_PASSWORD || 'drixl2024';

    if (password === correctPassword) {
      const expires = Date.now() + SESSION_DAYS * 24 * 60 * 60 * 1000;
      const payload = 'authenticated:' + expires;
      const sig = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('hex');
      const tokenValue = payload + ':' + sig;

      res.setHeader('Set-Cookie', cookie.serialize(COOKIE_NAME, tokenValue, {
        httpOnly: true,
        secure: true,
        sameSite: 'lax',
        path: '/',
        maxAge: SESSION_DAYS * 24 * 60 * 60
      }));
      res.writeHead(302, { Location: '/' });
      return res.end();
    } else {
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      return res.status(200).send(getLoginPage('Falsches Passwort'));
    }
  }

  // Handle logout
  if (reqPath === '/logout') {
    res.setHeader('Set-Cookie', cookie.serialize(COOKIE_NAME, '', {
      httpOnly: true,
      secure: true,
      sameSite: 'lax',
      path: '/',
      maxAge: 0
    }));
    res.writeHead(302, { Location: '/' });
    return res.end();
  }

  if (!isAuth) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(200).send(getLoginPage());
  }

  const page = loadPage(reqPath) || loadPage('/');
  if (page) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(200).send(page);
  }

  return res.status(404).send('Not found');
};
