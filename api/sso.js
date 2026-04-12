const crypto = require('crypto');
const cookie = require('cookie');

const SESSION_SECRET = process.env.SESSION_SECRET || 'drixl-default-secret-change-me';
const SSO_SECRET     = process.env.SSO_SECRET     || SESSION_SECRET;
const COOKIE_NAME    = 'drixl_session';
const SESSION_DAYS   = 30;

module.exports = async (req, res) => {
  // Accept GET with ?token=... 
  const url    = new URL(req.url, `https://${req.headers.host}`);
  const token  = url.searchParams.get('token');
  const redirect = url.searchParams.get('redirect') || '/';

  if (!token) {
    res.writeHead(302, { Location: '/' });
    return res.end();
  }

  // Verify SSO token: format is "expires:hmac"
  const parts = token.split(':');
  if (parts.length !== 2) {
    res.writeHead(302, { Location: '/' });
    return res.end();
  }

  const [expires, hmac] = parts;
  const expectedHmac = crypto
    .createHmac('sha256', SSO_SECRET)
    .update(`sso:${expires}`)
    .digest('hex');

  // Check signature and expiry (token valid for 5 minutes)
  if (hmac !== expectedHmac || Date.now() > parseInt(expires)) {
    res.writeHead(302, { Location: '/' });
    return res.end();
  }

  // Valid — create session cookie
  const sessionExpires = Date.now() + SESSION_DAYS * 86400000;
  const payload        = `authenticated:${sessionExpires}`;
  const sessionHmac    = crypto
    .createHmac('sha256', SESSION_SECRET)
    .update(payload)
    .digest('hex');

  res.setHeader('Set-Cookie', cookie.serialize(COOKIE_NAME, `${payload}:${sessionHmac}`, {
    httpOnly: true, secure: true, sameSite: 'lax', path: '/',
    maxAge: SESSION_DAYS * 86400,
  }));

  res.writeHead(302, { Location: decodeURIComponent(redirect) });
  return res.end();
};
