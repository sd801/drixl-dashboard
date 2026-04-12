const fs   = require('fs');
const path = require('path');

// Auth removed — access is controlled via Drixl Home SSO (apps.drixl.ch)
// Direct access still works without login

var ROUTES = {
  '/':               'index.html',
  '/index.html':     'index.html',
  '/cashflow':       'cashflow.html',
  '/vergleich':      'vergleich.html',
  '/erfolgsrechnung':'drixl-erfolgsrechnung.html'
};

var PAGE_CACHE = {};

function loadPage(route) {
  if (PAGE_CACHE[route]) return PAGE_CACHE[route];
  var filename = ROUTES[route];
  if (!filename) return null;
  try {
    var filePath = path.join(__dirname, '..', 'pages', filename);
    var content  = fs.readFileSync(filePath, 'utf8');
    PAGE_CACHE[route] = content;
    return content;
  } catch (e) { return null; }
}

module.exports = async function(req, res) {
  var reqPath = req.url.split('?')[0];
  if (reqPath === '' || reqPath === '/') reqPath = '/';

  if (reqPath === '/logout') {
    res.writeHead(302, { Location: 'https://apps.drixl.ch' });
    return res.end();
  }

  var page = loadPage(reqPath) || loadPage('/');
  if (page) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(200).send(page);
  }

  return res.status(404).send('Not found');
};
