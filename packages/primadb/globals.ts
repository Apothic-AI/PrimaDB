declare global {
  interface SymbolConstructor {
    readonly dispose: symbol;
  }
}

export {};
