import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import fs from 'node:fs'
import path from 'node:path'

// Thin dev config. The API base is not proxied here — the app talks to the control
// plane directly, and dev CORS is open on the control plane.
// Override the API host at runtime with ?api=.
//
// Encrypted transport (2026-08-22). The dashboard is served over HTTPS when the
// demo LAN's certificates exist, and over plain HTTP when they do not, matching
// the control plane's own rule in control-plane/Dockerfile. Generate them once
// with `python scripts/make_certs.py` and both ends move together.
//
// Why the dashboard itself has to move, and not just the calls it makes: a page
// delivered over plain HTTP can be rewritten in flight by anyone on the network,
// so a login form served that way can be altered to post the password elsewhere
// no matter how well encrypted the request it was supposed to make. Encrypting
// the calls while leaving the page that makes them unprotected would be a
// half-measure that reads as a whole one.
//
// src/api.js takes its scheme from the page's own protocol, so there is nothing
// else to switch: this file decides, and the API and the log socket follow.
const certDir = path.resolve(__dirname, '..', 'certs')
const certFile = path.join(certDir, 'server.pem')
const keyFile = path.join(certDir, 'server.key')
const haveCerts = fs.existsSync(certFile) && fs.existsSync(keyFile)

// Say which mode this is, rather than leaving it to be inferred from a URL that
// happens to work. A deployment that believed it was encrypted and was not is the
// failure this project refuses to ship quietly.
console.log(
  haveCerts
    ? '[fyp] dashboard: TLS ON  (https://localhost:5173)'
    : '[fyp] dashboard: TLS OFF (http://localhost:5173 — no ../certs/server.pem)',
)

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    ...(haveCerts
      ? { https: { cert: fs.readFileSync(certFile), key: fs.readFileSync(keyFile) } }
      : {}),
  },
})
