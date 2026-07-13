---
name: alpha-founder-qualification
description: Use only for the bounded Alpha Founder Centaur Gate B overlay attestation request.
---

# Alpha Founder Qualification

Use this skill only when the request asks for the Alpha Founder qualification overlay attestation.

1. Invoke `qualification_attestor` with command `attest` and the exact public nonce supplied by the user.
2. Do not access the network, credentials, environment variables, Kubernetes metadata, or unrelated files.
3. Return the tool result without inventing fields.
4. The immutable skill marker is `alpha-founder-overlay-skill-v1`.
