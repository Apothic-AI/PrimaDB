#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import {
  copyFileSync,
  cpSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, extname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const websiteDir = resolve(scriptDir, "..");
const repoRoot = resolve(websiteDir, "..");
const docsApiDir = resolve(repoRoot, "docs", "api");
const rustDocTargetDir = resolve(repoRoot, "target", "doc");
const rustStaticDir = resolve(websiteDir, "static", "rust-api");

function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd,
    stdio: "inherit",
    env: process.env,
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function ensureDir(path) {
  mkdirSync(path, { recursive: true });
}

function writeText(path, content) {
  ensureDir(dirname(path));
  if (existsSync(path) && readFileSync(path, "utf8") === content) {
    return;
  }
  writeFileSync(path, content);
}

function posixPath(path) {
  return path.split("\\").join("/");
}

function relativeFromRepo(path) {
  return posixPath(relative(repoRoot, path));
}

function cleanCodeBlock(code) {
  return code.trim().replace(/\r\n/g, "\n");
}

function kindLabel(node) {
  if (ts.isClassDeclaration(node)) return "class";
  if (ts.isInterfaceDeclaration(node)) return "interface";
  if (ts.isTypeAliasDeclaration(node)) return "type alias";
  if (ts.isFunctionDeclaration(node)) return "function";
  if (ts.isVariableStatement(node)) return "variable";
  if (ts.isEnumDeclaration(node)) return "enum";
  return "export";
}

function hasExportModifier(node) {
  return (ts.getModifiers(node) ?? []).some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword);
}

function getNodeName(node) {
  if (ts.isVariableStatement(node)) {
    return node.declarationList.declarations.map((declaration) => declaration.name.getText()).join(", ");
  }
  return node.name?.getText() ?? "default";
}

function printTsNode(sourceFile, node, printer) {
  const renderParameter = (parameter) => {
    const rest = parameter.dotDotDotToken ? "..." : "";
    const paramName = parameter.name.getText(sourceFile);
    const optional = parameter.questionToken || parameter.initializer ? "?" : "";
    let type = "unknown";
    if (parameter.type) {
      type = parameter.type.getText(sourceFile);
    } else if (parameter.initializer) {
      if (ts.isNumericLiteral(parameter.initializer)) {
        type = "number";
      } else if (
        parameter.initializer.kind === ts.SyntaxKind.TrueKeyword ||
        parameter.initializer.kind === ts.SyntaxKind.FalseKeyword
      ) {
        type = "boolean";
      } else if (ts.isStringLiteral(parameter.initializer)) {
        type = "string";
      }
    }
    return `${rest}${paramName}${optional}: ${type}`;
  };

  if (ts.isFunctionDeclaration(node)) {
    const name = node.name?.getText(sourceFile) ?? "default";
    const typeParameters = node.typeParameters?.length
      ? `<${node.typeParameters.map((parameter) => parameter.getText(sourceFile)).join(", ")}>`
      : "";
    const parameters = node.parameters.map(renderParameter).join(", ");
    const returnType = node.type ? `: ${node.type.getText(sourceFile)}` : "";
    return `export declare function ${name}${typeParameters}(${parameters})${returnType};`;
  }

  if (ts.isClassDeclaration(node)) {
    const name = node.name?.getText(sourceFile) ?? "default";
    const typeParameters = node.typeParameters?.length
      ? `<${node.typeParameters.map((parameter) => parameter.getText(sourceFile)).join(", ")}>`
      : "";
    const heritage = node.heritageClauses?.length
      ? ` ${node.heritageClauses.map((clause) => clause.getText(sourceFile)).join(" ")}`
      : "";
    const members = [];

    for (const member of node.members) {
      if (member.name && ts.isPrivateIdentifier(member.name)) {
        continue;
      }
      const modifiers = ts.getModifiers(member) ?? [];
      if (modifiers.some((modifier) => modifier.kind === ts.SyntaxKind.PrivateKeyword)) {
        continue;
      }
      const prefix = modifiers
        .filter((modifier) =>
          [ts.SyntaxKind.PublicKeyword, ts.SyntaxKind.ProtectedKeyword, ts.SyntaxKind.StaticKeyword, ts.SyntaxKind.ReadonlyKeyword].includes(
            modifier.kind,
          ),
        )
        .map((modifier) => modifier.getText(sourceFile))
        .join(" ");
      const prefixText = prefix ? `${prefix} ` : "";

      if (ts.isConstructorDeclaration(member)) {
        members.push(`    constructor(${member.parameters.map(renderParameter).join(", ")});`);
      } else if (ts.isMethodDeclaration(member) && member.name) {
        const methodName = member.name.getText(sourceFile);
        const optional = member.questionToken ? "?" : "";
        const typeParametersText = member.typeParameters?.length
          ? `<${member.typeParameters.map((parameter) => parameter.getText(sourceFile)).join(", ")}>`
          : "";
        const parameters = member.parameters.map(renderParameter).join(", ");
        const returnType = member.type ? member.type.getText(sourceFile) : "unknown";
        members.push(`    ${prefixText}${methodName}${optional}${typeParametersText}(${parameters}): ${returnType};`);
      } else if (ts.isPropertyDeclaration(member) && member.name) {
        const propertyName = member.name.getText(sourceFile);
        const optional = member.questionToken ? "?" : "";
        const type = member.type ? member.type.getText(sourceFile) : "unknown";
        members.push(`    ${prefixText}${propertyName}${optional}: ${type};`);
      }
    }

    return [`export declare class ${name}${typeParameters}${heritage} {`, ...members, "}"].join("\n");
  }

  return printer.printNode(ts.EmitHint.Unspecified, node, sourceFile);
}

function parseTsDeclarations(filePath) {
  const text = readFileSync(filePath, "utf8");
  const sourceFile = ts.createSourceFile(filePath, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const printer = ts.createPrinter({ newLine: ts.NewLineKind.LineFeed });
  const declarations = [];
  const reexports = [];

  for (const statement of sourceFile.statements) {
    if (ts.isExportDeclaration(statement) || ts.isExportAssignment(statement)) {
      reexports.push(cleanCodeBlock(printer.printNode(ts.EmitHint.Unspecified, statement, sourceFile)));
      continue;
    }

    if (!hasExportModifier(statement)) {
      continue;
    }

    declarations.push({
      name: getNodeName(statement),
      kind: kindLabel(statement),
      code: cleanCodeBlock(printTsNode(sourceFile, statement, printer)),
    });
  }

  return { declarations, reexports };
}

function renderTsPage({ title, sidebarPosition, intro, sources, extraSections = [] }) {
  const parts = [
    "---",
    `title: ${title}`,
    `sidebar_position: ${sidebarPosition}`,
    "---",
    "",
    intro.trim(),
    "",
    "> This page is generated from the current package source declarations.",
    "",
  ];

  for (const source of sources) {
    const parsed = parseTsDeclarations(source.path);
    parts.push(`## \`${relativeFromRepo(source.path)}\``);
    parts.push("");

    if (source.note) {
      parts.push(source.note.trim());
      parts.push("");
    }

    if (parsed.declarations.length > 0) {
      parts.push("### Direct exports");
      parts.push("");
      for (const declaration of parsed.declarations) {
        parts.push(`#### \`${declaration.name}\``);
        parts.push("");
        parts.push(`Kind: ${declaration.kind}`);
        parts.push("");
        parts.push("```ts");
        parts.push(declaration.code);
        parts.push("```");
        parts.push("");
      }
    }

    if (parsed.reexports.length > 0) {
      parts.push("### Re-exports");
      parts.push("");
      for (const reexport of parsed.reexports) {
        parts.push("```ts");
        parts.push(reexport);
        parts.push("```");
        parts.push("");
      }
    }
  }

  for (const section of extraSections) {
    parts.push(`## ${section.title}`);
    parts.push("");
    parts.push(section.body.trim());
    parts.push("");
  }

  return `${parts.join("\n").trim()}\n`;
}

function parsePythonStub(filePath) {
  const lines = readFileSync(filePath, "utf8").replace(/\r\n/g, "\n").split("\n");
  const blocks = [];
  let current = null;
  let pendingDecorators = [];

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (/^@\w/.test(line)) {
      if (current) {
        blocks.push(current);
        current = null;
      }
      pendingDecorators.push(line);
      continue;
    }

    if (/^(class|def)\s+/.test(line)) {
      if (current) {
        blocks.push(current);
      }
      current = {
        name: line.replace(/^(class|def)\s+/, "").split(/[(:]/, 1)[0].trim(),
        kind: line.startsWith("class ") ? "class" : "function",
        lines: [...pendingDecorators, line],
      };
      pendingDecorators = [];
      continue;
    }

    if (/^[A-Za-z_][A-Za-z0-9_]*\s*=/.test(line)) {
      if (current) {
        blocks.push(current);
        current = null;
      }
      blocks.push({
        name: line.split("=", 1)[0].trim(),
        kind: "type alias",
        lines: [line],
      });
      pendingDecorators = [];
      continue;
    }

    if (current) {
      current.lines.push(line);
    } else if (line.trim()) {
      pendingDecorators = [];
    }
  }

  if (current) {
    blocks.push(current);
  }

  return blocks.map((block) => ({
    ...block,
    code: cleanCodeBlock(block.lines.join("\n")),
  }));
}

function renderPythonPage({ title, sidebarPosition, intro, sourcePath, extraSections = [] }) {
  const blocks = parsePythonStub(sourcePath);
  const parts = [
    "---",
    `title: ${title}`,
    `sidebar_position: ${sidebarPosition}`,
    "---",
    "",
    intro.trim(),
    "",
    `> Generated from \`${relativeFromRepo(sourcePath)}\`.`,
    "",
  ];

  for (const block of blocks) {
    parts.push(`## \`${block.name}\``);
    parts.push("");
    parts.push(`Kind: ${block.kind}`);
    parts.push("");
    parts.push("```py");
    parts.push(block.code);
    parts.push("```");
    parts.push("");
  }

  for (const section of extraSections) {
    parts.push(`## ${section.title}`);
    parts.push("");
    parts.push(section.body.trim());
    parts.push("");
  }

  return `${parts.join("\n").trim()}\n`;
}

const strictConsistencyApiBody = [
  "PrimaDB is eventual/local-first by default. Strict consistency APIs are opt-in and scoped to a graph root.",
  "",
  "- `db.transaction(...)` applies a step array atomically on the local replica.",
  "- `db.scope(root).configure(...)` stores a scope policy for that root.",
  "- `scope.transaction(...)` runs a step array inside the scope and prefixes relative step paths with the scope root.",
  "- `consistency: \"local_transactional\"` marks the scope as a transaction boundary without adding network coordination.",
  "- `consistency: \"coordinated\"` requires the configured authority for canonical writes.",
  "- Non-authority peers use `offlineWrites: \"reject\"` to fail immediately or `offlineWrites: \"queue_provisional\"` to store a durable local proposal that normal reads and watches do not treat as committed graph state.",
  "- Relay sync clients expose `remoteTransaction(...)` / `remote_transaction(...)` to submit a coordinated transaction to an authority peer.",
  "",
  "The current coordinated implementation is a single-authority path. Quorum policies and strict authority read modes are represented in the policy model but are not full consensus or distributed multi-scope transactions yet.",
].join("\n");

const strictConsistencyRustBody = [
  "PrimaDB is eventual/local-first by default. Strict consistency APIs are opt-in and scoped to a graph root.",
  "",
  "- `Primadb::transaction(...)` runs a closure transaction atomically on the local replica.",
  "- `Primadb::apply_transaction_steps(...)` applies serializable step payloads used by SDKs and transports.",
  "- `Primadb::scope(root).configure(...)` stores a `ScopePolicy` for that root.",
  "- `Scope::transaction(...)` runs a Rust closure transaction inside the scope.",
  "- `Scope::transaction_steps(...)` runs step payloads inside the scope and can queue provisional proposals when configured.",
  "- `ScopeConsistency::LocalTransactional` marks a transaction boundary without network coordination.",
  "- `ScopeConsistency::Coordinated` requires the configured authority for canonical writes.",
  "",
  "The current coordinated implementation is a single-authority path. Quorum policy types exist, but quorum consensus, authority sequence certificates, and distributed multi-scope transactions are not implemented yet.",
].join("\n");

function splitTopLevel(text, delimiter = ",") {
  const parts = [];
  let current = "";
  let angleDepth = 0;
  let parenDepth = 0;
  let braceDepth = 0;
  let bracketDepth = 0;

  for (const character of text) {
    if (character === "<") angleDepth += 1;
    if (character === ">") angleDepth = Math.max(0, angleDepth - 1);
    if (character === "(") parenDepth += 1;
    if (character === ")") parenDepth = Math.max(0, parenDepth - 1);
    if (character === "{") braceDepth += 1;
    if (character === "}") braceDepth = Math.max(0, braceDepth - 1);
    if (character === "[") bracketDepth += 1;
    if (character === "]") bracketDepth = Math.max(0, bracketDepth - 1);

    if (
      character === delimiter &&
      angleDepth === 0 &&
      parenDepth === 0 &&
      braceDepth === 0 &&
      bracketDepth === 0
    ) {
      parts.push(current.trim());
      current = "";
      continue;
    }

    current += character;
  }

  if (current.trim()) {
    parts.push(current.trim());
  }

  return parts;
}

function extractRustResultInner(typeText) {
  const match = typeText.match(/^std::result::Result<([\s\S]+)>$/);
  if (!match) {
    return typeText;
  }
  const [okType] = splitTopLevel(match[1]);
  return okType ?? typeText;
}

function mapRustTypeToJs(typeText, classNames) {
  const compact = typeText.replace(/\s+/g, " ").trim();
  if (!compact || compact === "()") {
    return "void";
  }
  if (compact.startsWith("std::result::Result<")) {
    return mapRustTypeToJs(extractRustResultInner(compact), classNames);
  }
  if (compact.startsWith("Option<") && compact.endsWith(">")) {
    const inner = compact.slice("Option<".length, -1);
    return `${mapRustTypeToJs(inner, classNames)} | null`;
  }
  if (compact.includes("js_sys::Uint8Array")) {
    return "Uint8Array";
  }
  if (compact === "Vec<u8>") {
    return "Uint8Array";
  }
  if (compact.startsWith("Vec<") && compact.endsWith(">")) {
    const inner = compact.slice("Vec<".length, -1);
    return `${mapRustTypeToJs(inner, classNames)}[]`;
  }
  if (compact.includes("JsValue")) {
    return "any";
  }
  if (compact === "String" || compact === "&str" || compact === "&String") {
    return "string";
  }
  if (compact === "bool") {
    return "boolean";
  }
  if (/^(usize|u64|u32|u16|u8|isize|i64|i32|i16|i8|f64|f32)$/.test(compact)) {
    return "number";
  }
  if (compact.startsWith("&")) {
    return mapRustTypeToJs(compact.slice(1), classNames);
  }
  if (classNames.has(compact)) {
    return classNames.get(compact);
  }
  if (/^Wasm[A-Z]/.test(compact) && classNames.has(compact)) {
    return classNames.get(compact);
  }
  return compact
    .replace(/^crate::/, "")
    .replace(/^js_sys::/, "")
    .replace(/^web_sys::/, "")
    .replace(/^serde_json::Value as /, "")
    .replace(/^JsonValue$/, "any");
}

function parseRustFunctionSignature(signature, jsName, classNames) {
  const normalized = signature.replace(/\s+/g, " ").trim();
  const nameMatch = normalized.match(/pub\s+(async\s+)?fn\s+([A-Za-z0-9_]+)\s*\(/);
  if (!nameMatch) {
    return null;
  }

  const isAsync = Boolean(nameMatch[1]);
  const rustName = nameMatch[2];
  const paramsStart = normalized.indexOf("(", nameMatch.index);
  let paramsEnd = paramsStart;
  let depth = 0;
  for (let index = paramsStart; index < normalized.length; index += 1) {
    const character = normalized[index];
    if (character === "(") depth += 1;
    if (character === ")") {
      depth -= 1;
      if (depth === 0) {
        paramsEnd = index;
        break;
      }
    }
  }

  const paramsText = normalized.slice(paramsStart + 1, paramsEnd);
  let returnType = "void";
  const afterParams = normalized.slice(paramsEnd + 1);
  const returnMatch = afterParams.match(/->\s*([^ {][\s\S]*?)\s*\{$/);
  if (returnMatch) {
    returnType = mapRustTypeToJs(returnMatch[1], classNames);
  }

  const parameters = splitTopLevel(paramsText)
    .map((parameter) => parameter.trim())
    .filter((parameter) => parameter && parameter !== "&self" && parameter !== "&mut self")
    .map((parameter) => {
      const [name, ...typeParts] = parameter.split(":");
      if (typeParts.length === 0) {
        return parameter.trim();
      }
      return `${name.trim()}: ${mapRustTypeToJs(typeParts.join(":").trim(), classNames)}`;
    });

  const displayName = jsName ?? rustName;
  const signatureText =
    displayName === "constructor"
      ? `constructor(${parameters.join(", ")})`
      : `${displayName}(${parameters.join(", ")}): ${isAsync ? `Promise<${returnType}>` : returnType}`;

  return {
    rustName,
    jsName: displayName,
    signature: signatureText,
  };
}

function collectRustAttributes(lines, startIndex) {
  const attributes = [];
  let index = startIndex;
  while (index < lines.length) {
    const trimmed = lines[index].trim();
    if (!trimmed.startsWith("#[")) {
      break;
    }
    attributes.push(trimmed);
    index += 1;
  }
  return { attributes, nextIndex: index };
}

function extractJsName(attributes, key) {
  for (const attribute of attributes) {
    const match = attribute.match(new RegExp(`wasm_bindgen\\(${key} = ([^)]+)\\)`));
    if (match) {
      return match[1].replace(/"/g, "").trim();
    }
  }
  return null;
}

function extractRustSignature(lines, startIndex) {
  let index = startIndex;
  const signatureLines = [];
  while (index < lines.length) {
    signatureLines.push(lines[index].trim());
    if (lines[index].includes("{")) {
      break;
    }
    index += 1;
  }
  return {
    text: signatureLines.join(" "),
    nextIndex: index,
  };
}

function parseWasmBrowserRuntime(filePath) {
  const lines = readFileSync(filePath, "utf8").replace(/\r\n/g, "\n").split("\n");
  const classNames = new Map();

  for (let index = 0; index < lines.length; index += 1) {
    const { attributes, nextIndex } = collectRustAttributes(lines, index);
    const jsName = extractJsName(attributes, "js_name");
    if (!jsName) {
      continue;
    }
    const structMatch = lines[nextIndex]?.trim().match(/^pub struct (\w+)/);
    if (structMatch) {
      classNames.set(structMatch[1], jsName);
    }
  }

  const topLevelFunctions = [];
  const classes = [];
  const seenClasses = new Set();

  for (let index = 0; index < lines.length; index += 1) {
    const { attributes, nextIndex } = collectRustAttributes(lines, index);
    const jsName = extractJsName(attributes, "js_name");
    const jsClassName = extractJsName(attributes, "js_class");
    const nextLine = lines[nextIndex]?.trim() ?? "";

    if (jsName && /^pub (async )?fn /.test(nextLine)) {
      const signature = extractRustSignature(lines, nextIndex);
      const parsed = parseRustFunctionSignature(signature.text, jsName, classNames);
      if (parsed) {
        topLevelFunctions.push(parsed);
      }
      index = signature.nextIndex;
      continue;
    }

    if (!jsClassName) {
      continue;
    }

    const implMatch = nextLine.match(/^impl (\w+) \{$/);
    if (!implMatch) {
      continue;
    }

    const rustStructName = implMatch[1];
    if (seenClasses.has(jsClassName)) {
      continue;
    }
    seenClasses.add(jsClassName);

    const methods = [];
    let braceDepth = 1;
    index = nextIndex + 1;

    while (index < lines.length && braceDepth > 0) {
      const line = lines[index];
      const trimmed = line.trim();

      if (trimmed.startsWith("#[")) {
        const methodAttributes = [trimmed];
        let attributeIndex = index + 1;
        while (attributeIndex < lines.length && lines[attributeIndex].trim().startsWith("#[")) {
          methodAttributes.push(lines[attributeIndex].trim());
          attributeIndex += 1;
        }
        const methodLine = lines[attributeIndex]?.trim() ?? "";
        if (/^pub (async )?fn /.test(methodLine)) {
          const signature = extractRustSignature(lines, attributeIndex);
          const methodJsName =
            extractJsName(methodAttributes, "js_name") ||
            (methodAttributes.some((attribute) => attribute.includes("constructor")) ? "constructor" : null);
          const parsed = parseRustFunctionSignature(signature.text, methodJsName, classNames);
          if (parsed) {
            methods.push(parsed);
          }
          const signatureText = lines.slice(attributeIndex, signature.nextIndex + 1).join("\n");
          braceDepth += (signatureText.match(/\{/g) ?? []).length;
          braceDepth -= (signatureText.match(/\}/g) ?? []).length;
          index = signature.nextIndex + 1;
          continue;
        }
      }

      if (/^pub (async )?fn /.test(trimmed)) {
        const signature = extractRustSignature(lines, index);
        const parsed = parseRustFunctionSignature(signature.text, null, classNames);
        if (parsed) {
          methods.push(parsed);
        }
        const signatureText = lines.slice(index, signature.nextIndex + 1).join("\n");
        braceDepth += (signatureText.match(/\{/g) ?? []).length;
        braceDepth -= (signatureText.match(/\}/g) ?? []).length;
        index = signature.nextIndex + 1;
        continue;
      }

      braceDepth += (line.match(/\{/g) ?? []).length;
      braceDepth -= (line.match(/\}/g) ?? []).length;
      index += 1;
    }

    classes.push({
      name: jsClassName,
      rustStructName,
      methods,
    });
  }

  return { topLevelFunctions, classes };
}

function renderBrowserRuntimePage({ title, sidebarPosition, intro, sourcePath, extraSections = [] }) {
  const parsed = parseWasmBrowserRuntime(sourcePath);
  const parts = [
    "---",
    `title: ${title}`,
    `sidebar_position: ${sidebarPosition}`,
    "---",
    "",
    intro.trim(),
    "",
    `> Generated from \`${relativeFromRepo(sourcePath)}\`.`,
    "",
    "## Top-level functions",
    "",
  ];

  for (const fn of parsed.topLevelFunctions) {
    parts.push(`### \`${fn.jsName}\``);
    parts.push("");
    parts.push("```ts");
    parts.push(`function ${fn.signature};`);
    parts.push("```");
    parts.push("");
  }

  parts.push("## Runtime classes");
  parts.push("");
  for (const runtimeClass of parsed.classes) {
    parts.push(`### \`${runtimeClass.name}\``);
    parts.push("");
    parts.push("```ts");
    parts.push(`class ${runtimeClass.name} {`);
    for (const method of runtimeClass.methods) {
      parts.push(`  ${method.signature};`);
    }
    parts.push("}");
    parts.push("```");
    parts.push("");
  }

  for (const section of extraSections) {
    parts.push(`## ${section.title}`);
    parts.push("");
    parts.push(section.body.trim());
    parts.push("");
  }

  return `${parts.join("\n").trim()}\n`;
}

function parseRustReexports(filePath) {
  const source = readFileSync(filePath, "utf8");
  const groups = new Map();
  const groupRegex = /pub use (\w+)::\{([\s\S]*?)\};/g;
  const singleRegex = /pub use (\w+)::([^{;\n]+);/g;

  for (const match of source.matchAll(groupRegex)) {
    const moduleName = match[1];
    const items = splitTopLevel(match[2].replace(/\s+/g, " "))
      .map((item) => item.trim())
      .filter(Boolean);
    groups.set(moduleName, [...(groups.get(moduleName) ?? []), ...items]);
  }

  for (const match of source.matchAll(singleRegex)) {
    const moduleName = match[1];
    const item = match[2].trim();
    groups.set(moduleName, [...(groups.get(moduleName) ?? []), item]);
  }

  return [...groups.entries()]
    .map(([moduleName, items]) => [moduleName, [...new Set(items)].sort((left, right) => left.localeCompare(right))])
    .sort((left, right) => left[0].localeCompare(right[0]));
}

function copyRustDocSite() {
  run("cargo", ["doc", "--no-deps", "--package", "primadb"], repoRoot);

  const allowedDirectories = new Set(["primadb", "static.files", "src-files", "implementors"]);
  rmSync(rustStaticDir, { recursive: true, force: true });
  ensureDir(rustStaticDir);

  for (const entry of readdirSync(rustDocTargetDir, { withFileTypes: true })) {
    if (entry.name.startsWith(".")) {
      continue;
    }
    const source = resolve(rustDocTargetDir, entry.name);
    const destination = resolve(rustStaticDir, entry.name);
    if (entry.isDirectory()) {
      if (allowedDirectories.has(entry.name)) {
        cpSync(source, destination, { recursive: true });
      }
      continue;
    }

    const extension = extname(entry.name);
    if (
      extension === "" ||
      [".html", ".js", ".css", ".svg", ".png", ".ico", ".woff2", ".txt"].includes(extension)
    ) {
      copyFileSync(source, destination);
    }
  }
}

function renderRustPage({ title, sidebarPosition, intro, libPath, extraSections = [] }) {
  const groups = parseRustReexports(libPath);
  const parts = [
    "---",
    `title: ${title}`,
    `sidebar_position: ${sidebarPosition}`,
    "---",
    "",
    intro.trim(),
    "",
    `> The full crate rustdoc is bundled into this site from \`${relativeFromRepo(libPath)}\`.`,
    "",
    "## Full crate reference",
    "",
    '- <a href="/rust-api/primadb/" target="_blank" rel="noopener noreferrer">Open bundled rustdoc</a>',
    '- <a href="/rust-api/primadb/index.html" target="_blank" rel="noopener noreferrer">Open crate root page</a>',
    "",
    "## Re-export map",
    "",
  ];

  for (const [moduleName, items] of groups) {
    parts.push(`### \`${moduleName}\``);
    parts.push("");
    for (const item of items) {
      parts.push(`- \`${item}\``);
    }
    parts.push("");
  }

  for (const section of extraSections) {
    parts.push(`## ${section.title}`);
    parts.push("");
    parts.push(section.body.trim());
    parts.push("");
  }

  return `${parts.join("\n").trim()}\n`;
}

function generateApiDocs() {
  writeText(
    resolve(docsApiDir, "browser-typescript.md"),
    renderTsPage({
      title: "Browser TypeScript Package API",
      sidebarPosition: 2,
      intro:
        "This page covers the public `primadb` browser package entrypoint, hook helpers, and MoQ helpers. The re-exported runtime classes and transport bindings are documented on the browser runtime API page.",
      sources: [
        {
          path: resolve(repoRoot, "packages", "primadb", "index.ts"),
          note: "Primary `primadb` entrypoint.",
        },
        {
          path: resolve(repoRoot, "packages", "primadb", "moq.ts"),
          note: "Experimental `primadb/moq` helper entrypoint.",
        },
        {
          path: resolve(repoRoot, "packages", "primadb", "types.ts"),
          note: "Shared browser storage, blob, and keyed-record TypeScript helper types.",
        },
        {
          path: resolve(repoRoot, "packages", "primadb", "hooks.ts"),
          note: "Package-level hook helper types and registration utilities.",
        },
      ],
      extraSections: [
        {
          title: "Traversal semantics",
          body: [
            "Traversal methods are exported from the generated WASM runtime types.",
            "`traverse(...)` is local-first and bounded. Connected relay or mesh transports schedule missing linked nodes for background fetch, and traversal watches receive updates as those nodes arrive.",
          ].join("\n\n"),
        },
        {
          title: "Related pages",
          body:
            "- [Browser runtime API](browser-runtime)\n- [Threaded browser package API](browser-threads)\n- [Gun runtime API](gun-runtime-api)",
        },
      ],
    }),
  );

  writeText(
    resolve(docsApiDir, "browser-runtime.md"),
    renderBrowserRuntimePage({
      title: "Browser Runtime API",
      sidebarPosition: 3,
      intro:
        "This page covers the browser-facing `wasm_bindgen` runtime exported by the core crate. These are the classes and functions re-exported through the browser TypeScript package.",
      sourcePath: resolve(repoRoot, "src", "wasm.rs"),
      extraSections: [
        {
          title: "Browser segment persistence",
          body: [
            "`enableIndexedDbSegmentPersistence(...)` and `enableOpfsSegmentPersistence(...)` both perform an initial full flush, then auto-persist later data changes as incremental segment transactions.",
            "Segment persistence stores current graph state and storage transaction bookkeeping. It intentionally omits the transport pending-op queue, so high-churn opaque values are not duplicated into durable metadata on every save.",
            "Use OPFS segments for large or high-churn browser-local datasets when `navigator.storage.getDirectory()` is available. IndexedDB segments remain the compatibility path.",
            "`stats()` reports queued/coalesced events, successful and failed writes, full replacements, incremental transactions, entries written/deleted, estimated bytes written, and the last write error. `estimateStorage()` reports logical namespace size; OPFS also includes origin quota/usage when the browser exposes it.",
          ].join("\n\n"),
        },
        {
          title: "Keyed records",
          body: [
            "`putRecord(...)`, `putRecordBytes(...)`, `putRecordBlob(...)`, `getRecord(...)`, `scanRecords(...)`, `watchRecords(...)`, `applyRecordBatch(...)`, and `deleteRecord(...)` expose graph-native ordered records in the browser runtime.",
            "`remoteRecords(...)` and `watchRemoteRecords(...)` use the same record-scan request shape as local record watches, so relay and mesh transports do not define separate record semantics.",
            "Records persist through IndexedDB/OPFS segment persistence and use the same graph transaction, watch, sync, and blob paths as normal graph writes.",
          ].join("\n\n"),
        },
        {
          title: "Strict consistency and transactions",
          body: strictConsistencyApiBody.replaceAll(
            "`remoteTransaction(...)` / `remote_transaction(...)`",
            "`remoteTransaction(...)`",
          ),
        },
        {
          title: "Traversal semantics",
          body: [
            "`Chain.traverse(...)` returns the current local traversal result immediately. When connected relay or mesh transports are active, missing linked nodes are scheduled for bounded background fetch.",
            "`Chain.watchTraverse(...)` is the preferred API for peer-assisted traversal because it emits updated traversal results as fetched nodes merge into the local graph.",
            "`TraversalResult.fetched` is the number of background node fetches scheduled by that evaluation, not a blocking network completion count.",
          ].join("\n\n"),
        },
      ],
    }),
  );

  writeText(
    resolve(docsApiDir, "browser-threads.md"),
    renderTsPage({
      title: "Threaded Browser Package API",
      sidebarPosition: 4,
      intro:
        "This page covers the `primadb/threads` entrypoint. It extends the browser runtime with thread bootstrap helpers and still shares the same core runtime classes documented on the browser runtime page.",
      sources: [
        {
          path: resolve(repoRoot, "packages", "primadb", "threads.ts"),
          note: "Primary `primadb/threads` entrypoint.",
        },
      ],
      extraSections: [
        {
          title: "Thread pool bootstrap",
          body: [
            "The threaded build also re-exports the wasm thread-pool bootstrap helper when built with `wasm-threads`:",
            "",
            "```ts",
            "function initThreadPool(threads: number): Promise<void>;",
            "```",
            "",
            "Shared runtime classes such as `Primadb`, `Chain`, `WebSocketSync`, and `WebRtcMesh` are documented on the [browser runtime API](browser-runtime) page.",
          ].join("\n"),
        },
      ],
    }),
  );

  writeText(
    resolve(docsApiDir, "gun-runtime-api.md"),
    renderTsPage({
      title: "Gun Runtime API",
      sidebarPosition: 5,
      intro:
        "This page covers the browser Gun-compatible entrypoint and the typed runtime installer contract used by `primadb/gun`.",
      sources: [
        {
          path: resolve(repoRoot, "packages", "primadb", "gun.ts"),
          note: "Public `primadb/gun` entrypoint.",
        },
        {
          path: resolve(repoRoot, "packages", "primadb", "runtime", "primadb-gun.ts"),
          note: "Typed runtime installer surface.",
        },
      ],
    }),
  );

  writeText(
    resolve(docsApiDir, "node-package.md"),
    renderTsPage({
      title: "Node Package API",
      sidebarPosition: 6,
      intro:
        "This page covers the `primadb-node` native package surface. It is generated directly from the shipped TypeScript declaration files.",
      sources: [
        {
          path: resolve(repoRoot, "packages", "primadb-node", "index.d.ts"),
          note: "Published Node package declarations.",
        },
        {
          path: resolve(repoRoot, "packages", "primadb-node", "moq.d.ts"),
          note: "Experimental `primadb-node/moq` helper declarations.",
        },
      ],
      extraSections: [
        {
          title: "Strict consistency and transactions",
          body: strictConsistencyApiBody.replaceAll(
            "`remoteTransaction(...)` / `remote_transaction(...)`",
            "`remoteTransaction(...)`",
          ),
        },
        {
          title: "Traversal semantics",
          body: [
            "`Chain.traverse(...)` returns the current local traversal result immediately. With an active relay or mesh connection, missing linked nodes are scheduled for bounded background fetch.",
            "`Chain.watchTraverse(...)` receives updated traversal results as fetched nodes merge into the local graph.",
            "`TraversalResult.fetched` is the number of background node fetches scheduled by that evaluation.",
          ].join("\n\n"),
        },
      ],
    }),
  );

  writeText(
    resolve(docsApiDir, "python-package.md"),
    renderPythonPage({
      title: "Python Package API",
      sidebarPosition: 7,
      intro:
        "This page covers the `primadb-python` package surface. It is generated directly from the public stub file shipped with the package.",
      sourcePath: resolve(repoRoot, "packages", "primadb-python", "python", "primadb", "__init__.pyi"),
      extraSections: [
        {
          title: "Strict consistency and transactions",
          body: strictConsistencyApiBody.replaceAll("`remoteTransaction(...)` / `remote_transaction(...)`", "`remote_transaction(...)`"),
        },
        {
          title: "Traversal semantics",
          body: [
            "`Chain.traverse(...)` returns the current local traversal result immediately. With an active relay or mesh connection, missing linked nodes are scheduled for bounded background fetch.",
            "`Chain.watch_traverse(...)` receives updated traversal results as fetched nodes merge into the local graph.",
            "`TraversalResult.fetched` is the number of background node fetches scheduled by that evaluation.",
          ].join("\n\n"),
        },
      ],
    }),
  );

  copyRustDocSite();
  writeText(
    resolve(docsApiDir, "rust-crate.md"),
    renderRustPage({
      title: "Rust Crate API",
      sidebarPosition: 8,
      intro:
        "This page covers the public Rust crate surface. The site also serves the full bundled rustdoc so Rust consumers can browse the real crate API directly.",
      libPath: resolve(repoRoot, "src", "lib.rs"),
      extraSections: [
        {
          title: "Strict consistency and transactions",
          body: strictConsistencyRustBody,
        },
        {
          title: "Traversal semantics",
          body: [
            "`Chain::traverse` is local-first and bounded. With active relay or mesh transports, missing linked nodes are scheduled for bounded background fetch.",
            "`Chain::watch_traverse` receives updated results when fetched nodes merge into the local graph.",
            "The `fetched` field on `TraversalResult` is the number of background node fetches scheduled by that evaluation.",
          ].join("\n\n"),
        },
      ],
    }),
  );
}

generateApiDocs();
