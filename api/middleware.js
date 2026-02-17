const crypto = require('crypto');
const cookie = require('cookie');
const fs = require('fs');
const path = require('path');

const SESSION_SECRET = process.env.SESSION_SECRET || 'drixl-default-secret-change-me';
const APP_PASSWORD = process.env.APP_PASSWORD || 'drixl2025';
const COOKIE_NAME = 'drixl_session';
const SESSION_DAYS = 30;

function createToken() {
  const expires = Date.now() + SESSION_DAYS * 86400000;
  const payload = `authenticated:${expires}`;
  const hmac = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('hex');
  return `${payload}:${hmac}`;
}

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

function getLoginPage(error = '') {
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
  .field input::placeholder { color: #3a4a5a; }
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
    ${error ? `<div class="error">${error}</div>` : ''}
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

module.exports = async (req, res) => {
  const cookies = cookie.parse(req.headers.cookie || '');
  const token = cookies[COOKIE_NAME];
  const isAuth = verifyToken(token);
  
  // Parse URL
  let reqPath = req.url.split('?')[0];
  if (reqPath === '/' || reqPath === '') reqPath = '/index.html';
  
  // Not authenticated → show login
  if (!isAuth) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(200).send(getLoginPage());
  }
  
  // Authenticated → serve files
  const publicDir = path.join(__dirname, '..', 'public');
  const filePath = path.join(publicDir, reqPath);
  
  // Security: prevent path traversal
  if (!filePath.startsWith(publicDir)) {
    return res.status(403).send('Forbidden');
  }
  
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    const ext = path.extname(filePath);
    const types = { '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript', '.json': 'application/json' };
    res.setHeader('Content-Type', (types[ext] || 'text/plain') + '; charset=utf-8');
    return res.status(200).send(content);
  } catch (e) {
    // File not found → try index.html
    if (reqPath !== '/index.html') {
      try {
        const content = fs.readFileSync(path.join(publicDir, 'index.html'), 'utf8');
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        return res.status(200).send(content);
      } catch (e2) {}
    }
    return res.status(404).send('Not found');
  }
};
