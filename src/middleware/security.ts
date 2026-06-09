import helmet from "helmet";

// Security headers for a pure JSON API.
// No HTML is served so CSP can be strict: block all resource loading,
// deny framing, and strip the server fingerprint.
export const securityHeaders = helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc:     ["'none'"],
      frameAncestors: ["'none'"],
    },
  },
  // crossOriginEmbedderPolicy can break Supabase Storage CDN responses
  // seen by the browser if the API is used as a proxy; disable it.
  crossOriginEmbedderPolicy: false,
  // Allow cross-origin resource loads (storage URLs, CDN assets)
  crossOriginResourcePolicy: { policy: "cross-origin" },
});
