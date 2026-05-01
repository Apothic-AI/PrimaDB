
Build authenticated presence as a transport/session feature layered on existing crypto. Presence may advertise a public key, but hooks should only trust a new verifiedIdentity field after challenge/response succeeds.

1. Core Types
Add a new module, likely primadb/src/session_auth.rs:

- PresenceIdentity: advertised public key, alias, key scheme, optional claims.
- VerifiedIdentity: verified public key, alias, peer id, replica id, transport, session id, claims, issued/expires times, trust status.
- AuthChallenge: challenge id, nonce, issuing peer, target peer, transport, session id, issued time.
- AuthResponse: challenge id, responder public key, responder nonce, claims, signature.
- AuthTranscript: canonical signed payload covering both peer ids, replica ids, transport, session id, both nonces, public key, claims, timestamps, protocol version.

Extend primadb/src/router.rs with optional identity fields. Treat these as advertised, not trusted.

2. Handshake Protocol
For each new peer/session:

1. Peer sends presence with PresenceIdentity.
2. Receiver creates a nonce challenge.
3. Sender signs a canonical transcript with its private key.
4. Receiver verifies the signature against the advertised public key.
5. Receiver stores VerifiedIdentity for that peer/session.
6. Hooks receive verifiedIdentity when available.

The signature must bind to the live session, not just the public key. Include transport, route/channel, local session id, remote peer id, local peer id, both nonces, and expiry.

3. Transport Integration
Implement handshake in all transports that expose hooks:

- Native relay sync: primadb/src/native_sync.rs
- Native mesh: primadb/src/native_mesh.rs
- Browser relay/mesh: primadb/src/wasm.rs

Use new route payloads such as:

- AuthChallenge
- AuthResponse
- AuthVerified or local-only verification event

Do not trust the relay to authenticate peers unless relay attestation is added later. Relay can forward challenge/response; endpoints verify signatures themselves.

4. Hook Contexts
Extend hook contexts in primadb/src/hooks.rs:

- ConnectHookContext.verified_identity
- RoomHookContext.verified_identity
- ServeRequestContext.verified_identity
- ServeResultContext.verified_identity

Keep it optional. If no crypto identity is configured or verification has not completed, it is None.

This lets apps write policies like: allow reads on private/team-a only if verifiedIdentity.publicKey is in the team allowlist.

5. Configuration
Add auth/session options to relay and mesh configs:

- identity: local signing identity or reference to current authenticated local user.
- requireAuthenticatedPeers: reject unauthenticated peers before serving pull/watch/sync.
- trustedPublicKeys or trustedAliases: optional trust list.
- challengeTimeoutMs
- sessionTtlMs
- allowUnauthenticatedPresence: default true for compatibility during development, but apps can disable.

6. SDK Bindings
Expose matching types and config fields in:

- Browser TypeScript package
- Node package
- Python package
- Rust public API

Update generated docs so hook callback signatures show verifiedIdentity.

7. Security Rules
Important rules to enforce:

- Public key in presence is not trusted until challenge/response verifies.
- Challenges are single-use and expire quickly.
- Responses must sign both nonces and session metadata.
- Verification state is scoped to a connection/session, not globally forever.
- Peer id alone remains non-authoritative.
- Signed sync frames should continue to be validated independently; session auth improves connection trust but does not replace payload validation.

8. Tests
Add tests for:

- successful authenticated presence
- rejected invalid signature
- rejected replayed response
- rejected expired challenge
- hooks receiving verifiedIdentity
- hooks receiving no identity before verification
- relay-routed peer challenge/response
- WebRTC mesh challenge/response
- browser, Node, Python callback exposure
- unauthenticated peer denied when requireAuthenticatedPeers is enabled

Recommended Milestones

1. Add core session auth types and transcript signing/verification.
2. Add route payloads and native relay handshake.
3. Add hook context verifiedIdentity.
4. Add browser and mesh support.
5. Update Node/Python/browser bindings.
6. Add docs and examples showing ACL hooks using verified public keys.
