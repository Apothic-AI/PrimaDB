#!/usr/bin/env node

import {
  connectMeshViaMoqSession,
  createPrimadbMoqLoopback,
} from "../dist/moq.js";

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = predicate();
    if (value) {
      return value;
    }
    await wait(25);
  }
  return null;
}

function makeRoute(from, channel, payload) {
  return {
    route_id: `${from}/route/${Date.now().toString(16)}/${Math.random().toString(16).slice(2)}`,
    from,
    channel,
    target: { kind: "broadcast" },
    ttl: 6,
    hops: 0,
    issued_at_millis: Date.now(),
    reply_to: null,
    content_hash: null,
    seen_by: [from],
    payload,
  };
}

function createMeshDb(name, meshes) {
  return {
    replicaId() {
      return name;
    },
    pendingEnvelope() {
      return { ops: [] };
    },
    drainPendingEnvelope() {
      return { ops: [] };
    },
    applyEnvelope() {
      return 0;
    },
    connectMeshWithExternalSignaling(config, sendRoute) {
      const mesh = {
        accepted: [],
        closed: false,
        peerId() {
          return `mesh:${name}`;
        },
        signalingMode() {
          return "moq";
        },
        relayUrl() {
          return config.relayEndpoint?.url;
        },
        signalingReadyState() {
          return undefined;
        },
        peerCount() {
          return this.accepted.length;
        },
        openPeerCount() {
          return 0;
        },
        acceptSignalingRoute(route) {
          this.accepted.push(route);
        },
        announceSignalingPresence() {
          sendRoute(
            makeRoute(`mesh:${name}`, `mesh:${config.room}`, {
              kind: "signal",
              room: config.room,
              payload: {
                type: "join",
                room: config.room,
                from: `mesh:${name}`,
              },
            }),
          );
        },
        close() {
          this.closed = true;
        },
      };
      meshes.push(mesh);
      return mesh;
    },
  };
}

const room = `moq-mesh-smoke-${Date.now()}`;
const channel = `mesh:${room}`;
const publisherMeshes = [];
const subscriberMeshes = [];
const publisherDb = createMeshDb("publisher", publisherMeshes);
const subscriberDb = createMeshDb("subscriber", subscriberMeshes);
const link = await createPrimadbMoqLoopback({
  publisherDb,
  subscriberDb,
  path: `primadb/smoke/moq-mesh/${room}`,
  channel,
  intervalMs: 60_000,
});

let publisherMesh;
let subscriberMesh;
try {
  subscriberMesh = connectMeshViaMoqSession(subscriberDb, link.subscriber, {
    room,
    url: "moq://loopback",
    closeMoqSession: false,
  });
  publisherMesh = connectMeshViaMoqSession(publisherDb, link.publisher, {
    room,
    url: "moq://loopback",
    closeMoqSession: false,
  });

  const inbound = await waitFor(
    () => {
      publisherMesh.mesh.announceSignalingPresence();
      return subscriberMeshes[0]?.accepted.find((route) => route.payload?.kind === "signal");
    },
    5_000,
  );
  if (!inbound) {
    throw new Error("Timed out waiting for MoQ-backed mesh signaling route");
  }

  console.log(
    JSON.stringify(
      {
        room,
        channel,
        publisherSignaling: publisherMesh.mesh.signalingMode(),
        subscriberAcceptedRoutes: subscriberMeshes[0].accepted.length,
        inboundPayload: inbound.payload.kind,
      },
      null,
      2,
    ),
  );
} finally {
  publisherMesh?.close();
  subscriberMesh?.close();
  link.close();
}
