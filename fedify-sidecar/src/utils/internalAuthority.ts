import { isIP } from "node:net";

const loopbackHostnames = new Set(["localhost", "127.0.0.1", "::1", "host.docker.internal"]);

export function isSecureOrTrustedInternalUrl(url: URL): boolean {
  return url.protocol === "https:"
    || (url.protocol === "http:" && isTrustedInternalHostname(url.hostname));
}

export function isTrustedInternalHostname(hostname: string): boolean {
  const normalized = normalizeHostname(hostname);
  if (!normalized) {
    return false;
  }

  if (loopbackHostnames.has(normalized)) {
    return true;
  }

  const ipVersion = isIP(normalized);
  if (ipVersion === 4) {
    return isPrivateIpv4(normalized);
  }
  if (ipVersion === 6) {
    return isPrivateIpv6(normalized);
  }

  // Docker Compose and same-namespace service discovery commonly use
  // single-label hostnames like `activitypods` or `mock-activitypods`.
  return !normalized.includes(".");
}

function normalizeHostname(hostname: string): string {
  return hostname.trim().replace(/^\[/, "").replace(/\]$/, "").toLowerCase();
}

function isPrivateIpv4(hostname: string): boolean {
  const octets = hostname.split(".").map((part) => Number.parseInt(part, 10));
  if (octets.length !== 4 || octets.some((part) => Number.isNaN(part))) {
    return false;
  }

  const [first, second] = octets;
  return first === 10
    || first === 127
    || (first === 172 && second !== undefined && second >= 16 && second <= 31)
    || (first === 192 && second === 168)
    || (first === 169 && second === 254);
}

function isPrivateIpv6(hostname: string): boolean {
  return hostname === "::1"
    || hostname.startsWith("fc")
    || hostname.startsWith("fd")
    || hostname.startsWith("fe80:");
}
