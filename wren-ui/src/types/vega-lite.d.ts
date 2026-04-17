declare module 'vega-lite' {
  export type TopLevelSpec = any;
  export type Config = any;
  export function compile(...args: any[]): { spec: any };
}
