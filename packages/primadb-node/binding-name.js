import { familySync, MUSL } from "detect-libc";

function linuxAbi() {
  return familySync() === MUSL ? "musl" : "gnu";
}

export function currentBindingName(stem = "primadb-node") {
  switch (process.platform) {
    case "linux":
      return `${stem}.linux-${process.arch}-${linuxAbi()}.node`;
    case "darwin":
      return `${stem}.darwin-${process.arch}.node`;
    case "win32":
      return `${stem}.win32-${process.arch}-msvc.node`;
    default:
      throw new Error(`Unsupported platform: ${process.platform}/${process.arch}`);
  }
}

export function currentCargoLibraryName() {
  switch (process.platform) {
    case "linux":
      return "libprimadb_node.so";
    case "darwin":
      return "libprimadb_node.dylib";
    case "win32":
      return "primadb_node.dll";
    default:
      throw new Error(`Unsupported platform: ${process.platform}/${process.arch}`);
  }
}
