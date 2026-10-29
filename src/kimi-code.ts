import { isNonEmptyString } from "./common.ts";

// Moonshot's official Kimi Code hosts (China and international). Pi 1.3.0
// ships kimi-coding as openai-completions at `<host>/coding/v1`; older Pi
// versions used anthropic-messages at `<host>/coding`.
const OFFICIAL_KIMI_CODE_HOSTS = new Set(["api.kimi.com", "api.kimi.ai"]);

export type OfficialKimiCodeBaseUrl = {
  host: string;
  /** Whether the path is `/coding/v1` (true) or bare `/coding` (false). */
  hasV1: boolean;
};

/**
 * Parse an official Kimi Code base URL: https, one of the official hosts, no
 * port, credentials, query or fragment, and a path of exactly `/coding` or
 * `/coding/v1` (trailing slashes ignored). Anything else, including proxies
 * that mirror the path, returns undefined.
 */
export function parseOfficialKimiCodeBaseUrl(baseUrl: unknown): OfficialKimiCodeBaseUrl | undefined {
  if (!isNonEmptyString(baseUrl)) return undefined;
  let url: URL;
  try {
    url = new URL(baseUrl.trim());
  } catch {
    return undefined;
  }
  if (url.protocol !== "https:" || url.port !== "" || url.username !== "" || url.password !== "") return undefined;
  if (url.search !== "" || url.hash !== "" || /[?#]/.test(baseUrl)) return undefined;
  const host = url.hostname.toLowerCase();
  if (!OFFICIAL_KIMI_CODE_HOSTS.has(host)) return undefined;
  const path = url.pathname.replace(/\/+$/, "").toLowerCase();
  if (path === "/coding") return { host, hasV1: false };
  if (path === "/coding/v1") return { host, hasV1: true };
  return undefined;
}

/** Strip surrounding whitespace and trailing slashes for base URL comparison. */
export function normalizeBaseUrlForComparison(baseUrl: string): string {
  return baseUrl.trim().replace(/\/+$/, "");
}

/** The `/coding/v1` form of a bare official `/coding` base URL, keeping its spelling. */
export function kimiCodeV1BaseUrl(baseUrl: string): string | undefined {
  const parsed = parseOfficialKimiCodeBaseUrl(baseUrl);
  if (!parsed || parsed.hasV1) return undefined;
  return `${normalizeBaseUrlForComparison(baseUrl)}/v1`;
}

/** A receipt-recorded base URL change is exactly "bare official /coding" → "+ /v1". */
export function isKimiCodeV1BaseUrlChange(before: unknown, after: unknown): boolean {
  return typeof before === "string" && typeof after === "string" && kimiCodeV1BaseUrl(before) === after;
}
