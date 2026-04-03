# Workspace

## Telegram Automation Dashboard

### Overview

A full-featured Telegram account management dashboard built with **Python Flask + Telethon**.
Runs alongside the pnpm monorepo as a root-level Python app.

### Running the Flask App

- **Start command**: `python main.py`
- **Port**: 5000 (reads `PORT` env var)
- **Default login**: `admin` / `admin123`
- **Database**: SQLite (`telegram_automation.db`)

### Flask App Features

- Multi-account Telegram management (add accounts via phone + OTP)
- Group Broadcast (text, photo, video) with scheduling
- Personal chats viewer with messaging
- Bulk DMs and DM campaigns with personalization
- Auto-join groups (single or all accounts)
- Search & join groups by keyword
- Group link scraper
- Member scraper (with activity filtering)
- Multi-account member adding
- Multi-account reporting (strong: FloodWait recovery, configurable delays, stop button, multi-post targeting, smart URL parsing, task cancellation)
- Account tools (profile update, mark read, online status, etc.)
- Proxy support (SOCKS4/5, HTTP) per-account or global
- Subscription system with license keys (free/basic/pro/unlimited)
- Crypto payment flow (USDT/BTC/ETH/TRX) with admin approval
- Admin panel at `/admin` for user and key management
- Admin: ban/unban users, activity logs, login history, live stats monitoring
- Inbox viewer (read chats and messages from any connected account)
- Auto-Reply Rules (keyword→reply with contains/exact/startswith matching)
- Blacklist management (per-user list of blocked identifiers)
- Account health check (live Telethon ping to detect banned accounts)
- Session backup/export and restore/import as JSON
- Login history tracking (IP + status per login attempt)
- Interval scheduler (repeat broadcasts every N minutes, auto-reschedule)
- Contact info: Owner = MR ADS, Telegram = @trustedzone139, Channel = t.me/TrustedZoneOfficial

### Admin Credentials
- Admin: `admin` / `Rasel412`
- User: `user` / `Rasel412`

### DB Models
- User (is_banned, ban_reason, last_login_at, last_login_ip)
- LoginHistory (user_id, ip_address, status, timestamp)
- AutoReplyRule (user_id, account_id, keyword, reply_text, match_type, is_active, trigger_count)
- MessageBlacklist (user_id, identifier, reason, added_at)
- ScheduledBroadcast (repeat_interval_minutes, next_run_at, run_count)

### Templates

- `templates/login.html` - Login/register page
- `templates/dashboard.html` - Main dashboard (all features)
- `templates/admin.html` - Admin panel

### Python Dependencies

Managed via uv (`.pythonlibs/`): flask, flask-sqlalchemy, flask-login, werkzeug, telethon, pysocks, aiohttp, cryptography, python-dotenv

---

## Monorepo Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

## Structure

```text
artifacts-monorepo/
├── artifacts/              # Deployable applications
│   └── api-server/         # Express API server
├── lib/                    # Shared libraries
│   ├── api-spec/           # OpenAPI spec + Orval codegen config
│   ├── api-client-react/   # Generated React Query hooks
│   ├── api-zod/            # Generated Zod schemas from OpenAPI
│   └── db/                 # Drizzle ORM schema + DB connection
├── scripts/                # Utility scripts (single workspace package)
│   └── src/                # Individual .ts scripts, run via `pnpm --filter @workspace/scripts run <script>`
├── pnpm-workspace.yaml     # pnpm workspace (artifacts/*, lib/*, lib/integrations/*, scripts)
├── tsconfig.base.json      # Shared TS options (composite, bundler resolution, es2022)
├── tsconfig.json           # Root TS project references
└── package.json            # Root package with hoisted devDeps
```

## TypeScript & Composite Projects

Every package extends `tsconfig.base.json` which sets `composite: true`. The root `tsconfig.json` lists all packages as project references. This means:

- **Always typecheck from the root** — run `pnpm run typecheck` (which runs `tsc --build --emitDeclarationOnly`). This builds the full dependency graph so that cross-package imports resolve correctly. Running `tsc` inside a single package will fail if its dependencies haven't been built yet.
- **`emitDeclarationOnly`** — we only emit `.d.ts` files during typecheck; actual JS bundling is handled by esbuild/tsx/vite...etc, not `tsc`.
- **Project references** — when package A depends on package B, A's `tsconfig.json` must list B in its `references` array. `tsc --build` uses this to determine build order and skip up-to-date packages.

## Root Scripts

- `pnpm run build` — runs `typecheck` first, then recursively runs `build` in all packages that define it
- `pnpm run typecheck` — runs `tsc --build --emitDeclarationOnly` using project references

## Packages

### `artifacts/api-server` (`@workspace/api-server`)

Express 5 API server. Routes live in `src/routes/` and use `@workspace/api-zod` for request and response validation and `@workspace/db` for persistence.

- Entry: `src/index.ts` — reads `PORT`, starts Express
- App setup: `src/app.ts` — mounts CORS, JSON/urlencoded parsing, routes at `/api`
- Routes: `src/routes/index.ts` mounts sub-routers; `src/routes/health.ts` exposes `GET /health` (full path: `/api/health`)
- Depends on: `@workspace/db`, `@workspace/api-zod`
- `pnpm --filter @workspace/api-server run dev` — run the dev server
- `pnpm --filter @workspace/api-server run build` — production esbuild bundle (`dist/index.cjs`)
- Build bundles an allowlist of deps (express, cors, pg, drizzle-orm, zod, etc.) and externalizes the rest

### `lib/db` (`@workspace/db`)

Database layer using Drizzle ORM with PostgreSQL. Exports a Drizzle client instance and schema models.

- `src/index.ts` — creates a `Pool` + Drizzle instance, exports schema
- `src/schema/index.ts` — barrel re-export of all models
- `src/schema/<modelname>.ts` — table definitions with `drizzle-zod` insert schemas (no models definitions exist right now)
- `drizzle.config.ts` — Drizzle Kit config (requires `DATABASE_URL`, automatically provided by Replit)
- Exports: `.` (pool, db, schema), `./schema` (schema only)

Production migrations are handled by Replit when publishing. In development, we just use `pnpm --filter @workspace/db run push`, and we fallback to `pnpm --filter @workspace/db run push-force`.

### `lib/api-spec` (`@workspace/api-spec`)

Owns the OpenAPI 3.1 spec (`openapi.yaml`) and the Orval config (`orval.config.ts`). Running codegen produces output into two sibling packages:

1. `lib/api-client-react/src/generated/` — React Query hooks + fetch client
2. `lib/api-zod/src/generated/` — Zod schemas

Run codegen: `pnpm --filter @workspace/api-spec run codegen`

### `lib/api-zod` (`@workspace/api-zod`)

Generated Zod schemas from the OpenAPI spec (e.g. `HealthCheckResponse`). Used by `api-server` for response validation.

### `lib/api-client-react` (`@workspace/api-client-react`)

Generated React Query hooks and fetch client from the OpenAPI spec (e.g. `useHealthCheck`, `healthCheck`).

### `scripts` (`@workspace/scripts`)

Utility scripts package. Each script is a `.ts` file in `src/` with a corresponding npm script in `package.json`. Run scripts via `pnpm --filter @workspace/scripts run <script>`. Scripts can import any workspace package (e.g., `@workspace/db`) by adding it as a dependency in `scripts/package.json`.
