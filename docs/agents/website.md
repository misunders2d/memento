# Public information website (Vercel)

Detail for [AGENTS.md](../../AGENTS.md). Caveman-compressed; every rule here is binding.

## Public information website (static Vercel hosting)

`website/`: only public product, privacy, terms pages; not memory dashboard or Google sign-in. Import repository into Vercel: **Root Directory `website`**, **Framework Preset Other**, and **Output Directory `.`**. Checked-in `vercel.json` disables installation + build commands, sets response headers + canonical trailing slashes. No dependencies, environment variables, functions, API rewrites, server containers. Do not deploy the repository root.

Local routing preview, current Vercel CLI, without linking a project or deploying:
```sh
vercel dev --local --cwd website --listen 127.0.0.1:18765
```

Add `memento.bezosapp.uk` to Vercel project's production domains. In Cloudflare `bezosapp.uk` zone add `memento` **DNS-only CNAME using the exact project-specific target Vercel provides**. Keep existing nameservers + other records. Keep production pages publicly readable, without Vercel authentication challenge, so Google can fetch policy URLs.

No Contabo changes needed for this website: do not add a Cloudflare Tunnel route, run an extra website container, open public ports, or point this hostname at the private memory API. Memory service stays private, independent.

After pages publicly reachable, use these Google Auth Platform branding values in existing project:
- Application home page: `https://memento.bezosapp.uk/`
- Privacy policy: `https://memento.bezosapp.uk/privacy/`
- Terms of service: `https://memento.bezosapp.uk/terms/`
- Authorized domain: `bezosapp.uk`

Before requesting Google brand verification: verify `bezosapp.uk` ownership in Google Search Console with Google account associated with this project as Owner or Editor. If needed, add Google's exact verification TXT record in Cloudflare without changing other DNS records. Personal-use apps can be exempt from review; hosting pages does not imply domain verification or branding approval. See [Google's authorized-domain requirements](https://developers.google.com/identity/protocols/oauth2/production-readiness/brand-verification#authorized-domains).

Keep existing **Desktop app** OAuth client + loopback callback; site needs no Web client or public OAuth callback. Publishing External app **In production** avoids seven-day Testing refresh-token limitation. If connection authorized in Testing: have human re-consent after publishing and install private connection as described in [drive-admin-recovery.md](drive-admin-recovery.md). Public pages, project administration, publishing do not themselves grant `drive.file` access; native Google consent stays explicit human step.
