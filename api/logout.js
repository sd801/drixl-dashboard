const cookie = require('cookie');

const COOKIE_NAME = 'drixl_session';

module.exports = async (req, res) => {
  res.setHeader('Set-Cookie', cookie.serialize(COOKIE_NAME, '', {
    httpOnly: true,
    secure: true,
    sameSite: 'lax',
    path: '/',
    maxAge: 0,
  }));
  res.writeHead(302, { Location: '/' });
  return res.end();
};
