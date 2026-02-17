# Drixl Transporte – Finance Dashboard

Passwortgeschütztes Finance Dashboard mit bexio-Daten via Supabase.

## Setup

### 1. GitHub Repository erstellen

```bash
cd drixl-dashboard
git init
git add .
git commit -m "Initial dashboard deployment"
git remote add origin https://github.com/DEIN-USERNAME/drixl-dashboard.git
git push -u origin main
```

### 2. Vercel Deployment

1. Gehe zu [vercel.com/new](https://vercel.com/new)
2. Importiere das GitHub Repository
3. **Environment Variables** setzen (wichtig!):
   - `APP_PASSWORD` → Dein gewünschtes Login-Passwort
   - `SESSION_SECRET` → Ein langer zufälliger String (z.B. `openssl rand -hex 32`)
4. Deploy klicken

### 3. Fertig!

- Dashboard: `https://drixl-dashboard.vercel.app/`
- Vergleich: `https://drixl-dashboard.vercel.app/vergleich.html`
- Login wird automatisch angezeigt wenn nicht eingeloggt
- Session hält 30 Tage

## Struktur

```
├── api/
│   ├── middleware.js    # Auth-Check + File-Serving
│   ├── login.js         # POST /api/login
│   └── logout.js        # GET /api/logout
├── public/
│   ├── index.html       # Haupt-Dashboard
│   └── vergleich.html   # Periodenvergleich
├── vercel.json          # Routing-Config
└── package.json
```

## Passwort ändern

In Vercel → Settings → Environment Variables → `APP_PASSWORD` ändern → Redeploy.
