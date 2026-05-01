import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { currentBindingName } from "./binding-name.js";

const require = createRequire(import.meta.url);
const packageDir = dirname(fileURLToPath(import.meta.url));
const bindingPath = join(packageDir, currentBindingName());
const native = require(bindingPath);

export const {
  Primadb,
  Chain,
  Scope,
  Subscription,
  RelayServer,
  RemoteWatch,
  WebSocketSync,
  WebRtcMesh,
  generateIdentity,
} = native;

export default native;
