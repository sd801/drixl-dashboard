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
  .login-box { width: 100%; max-width: 380px; padding: 20px; }
  .logo { text-align: center; margin-bottom: 40px; }
  .logo-sub { font-size: 11px; font-weight: 600; letter-spacing: 0.14em; text-transform: uppercase; 
    color: #2a6f4e; font-family: 'JetBrains Mono', monospace; margin-bottom: 6px; }
  .logo h1 { font-size: 26px; font-weight: 700; color: #e8edf2; }
  .card { background: #111820; border: 1px solid #1a2332; border-radius: 16px; padding: 32px 28px; }
  .card-title { font-size: 14px; font-weight: 600; color: #8a9aaa; margin-bottom: 24px; text-align: center; }
  .field { margin-bottom: 20px; }
  .field label { display: block; font-size: 11px; font-weight: 600; letter-spacing: 0.08em; 
    text-transform: uppercase; color: #5a6a7a; font-family: 'JetBrains Mono', monospace; margin-bottom: 8px; }
  .field input { width: 100%; background: #0a1018; border: 1px solid #1a2332; color: #e8edf2; 
    font-family: 'JetBrains Mono', monospace; font-size: 14px; padding: 12px 16px; border-radius: 8px; 
    outline: none; transition: border 0.2s; }
  .field input:focus { border-color: #2a6f4e; }
  .btn { width: 100%; padding: 12px; border-radius: 8px; border: none; cursor: pointer; 
    font-size: 14px; font-weight: 700; font-family: 'JetBrains Mono', monospace; 
    background: linear-gradient(135deg, #2a6f4e, #4ecdc4); color: #0a1018; 
    transition: all 0.2s; letter-spacing: 0.04em; }
  .btn:hover { transform: translateY(-1px); box-shadow: 0 4px 20px rgba(78,205,196,0.3); }
  .error { background: #1a1215; border: 1px solid #4a2020; color: #ff6b6b; font-size: 12px; 
    padding: 10px 14px; border-radius: 8px; margin-bottom: 16px; text-align: center; 
    font-family: 'JetBrains Mono', monospace; }
  .footer { text-align: center; margin-top: 32px; color: #2a3a4a; font-size: 11px; 
    font-family: 'JetBrains Mono', monospace; }
</style>
</head>
<body>
<div class="login-box">
  <div class="logo">
    <div class="logo-sub">Drixl Transporte GmbH</div>
    <h1>Finance Dashboard</h1>
  </div>
  <div class="card">
    <div class="card-title">Zugang zum Dashboard</div>
    ${error ? '<div class="error">' + error + '</div>' : ''}
    <form method="POST" action="/api/login">
      <div class="field">
        <label>Passwort</label>
        <input type="password" name="password" placeholder="Passwort eingeben" autofocus required>
      </div>
      <button type="submit" class="btn">Anmelden</button>
    </form>
  </div>
  <div class="footer">Geschützter Bereich · Nur für autorisierte Benutzer</div>
</div>
</body>
</html>`;
}

// Pre-load pages into memory at module init
// The includeFiles config in vercel.json ensures these are available
const PAGES = {};
const pagesDir = path.join(process.cwd(), 'pages');
try {
  PAGES['/'] = fs.readFileSync(path.join(pagesDir, 'index.html'), 'utf8');
  PAGES['/index.html'] = PAGES['/'];
  PAGES['/vergleich.html'] = fs.readFileSync(path.join(pagesDir, 'vergleich.html'), 'utf8');
  console.log('Pages loaded successfully:', Object.keys(PAGES).join(', '));
} catch (e) {
  // Fallback: try relative to __dirname
  try {
    const altDir = path.join(__dirname, '..', 'pages');
    PAGES['/'] = fs.readFileSync(path.join(altDir, 'index.html'), 'utf8');
    PAGES['/index.html'] = PAGES['/'];
    PAGES['/vergleich.html'] = fs.readFileSync(path.join(altDir, 'vergleich.html'), 'utf8');
    console.log('Pages loaded via __dirname fallback');
  } catch (e2) {
    console.error('Could not load pages from either path:', e.message, e2.message);
  }
}

module.exports = async (req, res) => {
  const cookies = cookie.parse(req.headers.cookie || '');
  const token = cookies[COOKIE_NAME];
  const isAuth = verifyToken(token);
  
  let reqPath = req.url.split('?')[0];
  if (reqPath === '' || reqPath === '/') reqPath = '/';
  
  // Not authenticated → login
  if (!isAuth) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(200).send(getLoginPage());
  }
  
  // Authenticated → serve page
  const page = PAGES[reqPath] || PAGES['/'];
  if (page) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(200).send(page);
  }
  
  return res.status(404).send('Page not found. Available: ' + Object.keys(PAGES).join(', '));
};
