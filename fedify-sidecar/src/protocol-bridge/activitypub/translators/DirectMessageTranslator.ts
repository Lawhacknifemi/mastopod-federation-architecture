import type { CanonicalDirectMessageIntent, CanonicalIntent } from "../../canonical/CanonicalIntent.js";
import type { CanonicalFacet } from "../../canonical/CanonicalContent.js";
import type { TranslationContext } from "../../ports/ProtocolBridgePorts.js";
import type { ProtocolTranslator } from "../../registry/TranslatorRegistry.js";
import { buildCanonicalIntentId } from "../../idempotency/CanonicalIntentIdBuilder.js";
import { htmlToCanonicalBlocks } from "../../text/HtmlToCanonicalBlocks.js";
import { collectApCustomEmojis } from "../../../utils/apCustomEmojis.js";
import { fetchOpenGraph } from "../../../utils/opengraph.js";
import {
  buildAttachments,
  buildTagFacets,
  buildInlineLinkAndTagFacets,
  mergeFacets,
  resolveOptionalObjectRef,
  getActorId,
  extractFirstUrl,
  asString,
} from "./shared.js";
import { z } from "zod";

const actorSchema = z.union([
  z.string().min(1),
  z.object({ id: z.string().min(1) }).passthrough(),
]);

const recipientListSchema = z.union([
  z.string().url(),
  z.array(z.string().url()),
]).optional();

const directMessageActivitySchema = z.object({
  id: z.string().min(1),
  type: z.literal("Create"),
  actor: actorSchema,
  published: z.string().optional(),
  to: recipientListSchema,
  cc: recipientListSchema,
  object: z.object({
    type: z.literal("Note"),
    content: z.string(),
    attributedTo: z.string().url().optional(),
    id: z.string().optional(),
    published: z.string().optional(),
    inReplyTo: z.unknown().optional(),
    context: z.unknown().optional(),
    url: z.unknown().optional(),
    tag: z.unknown().optional(),
    attachment: z.unknown().optional(),
    quote: z.string().optional(),
    quoteUrl: z.string().optional(),
    quoteUri: z.string().optional(),
    _misskey_quote: z.string().optional(),
  }).passthrough().optional(),
});

export type DirectMessageActivity = z.infer<typeof directMessageActivitySchema>;

/** Maximum allowed message text length. */
const MAX_TEXT_LEN = 10_000;

/** Public streams indicator from the ActivityStreams namespace. */
const PUBLIC_AUDIENCE = "https://www.w3.org/ns/activitystreams#Public";

function toList(raw: string | string[] | undefined): string[] {
  if (!raw) return [];
  return Array.isArray(raw) ? raw : [raw];
}

/**
 * Translator for ActivityPub direct messages (1-to-1 and group DMs).
 *
 * Matches `Create(Note)` addressed exclusively to non-public recipients.
 * Must be registered before `CreateNoteTranslator` since both match `Create(Note)`.
 *
 * Rich content extracted per-message:
 *   - Attachments (image, audio, video, GIF, document)
 *   - Facets: mentions (scoped to conversation participants), private hashtags, links
 *   - Custom emoji
 *   - Link preview (first link-type facet URL)
 *   - inReplyTo / replyRoot / quoteOf for threading and quote-posts
 */
export class DirectMessageTranslator implements ProtocolTranslator<unknown> {
  public supports(input: unknown): boolean {
    try {
      const parsed = directMessageActivitySchema.safeParse(input);
      if (!parsed.success) return false;

      const activity = parsed.data;
      const toList_ = toList(activity.to);

      // Must have exactly one recipient, and it must not be public
      return toList_.length === 1 && toList_.every((r) => r !== PUBLIC_AUDIENCE);
    } catch {
      return false;
    }
  }

  public async translate(
    input: unknown,
    ctx: TranslationContext,
  ): Promise<CanonicalIntent | null> {
    return translateDirectMessageActivity(input, ctx);
  }
}

/**
 * Translate an ActivityPub `Create(Note)` direct message to a
 * `CanonicalDirectMessageIntent` with full rich-content extraction.
 */
export async function translateDirectMessageActivity(
  input: unknown,
  ctx: TranslationContext,
): Promise<CanonicalDirectMessageIntent | null> {
  const parsed = directMessageActivitySchema.safeParse(input);
  if (!parsed.success) return null;

  const activity = parsed.data;
  const toRecipients = toList(activity.to).filter((r) => r !== PUBLIC_AUDIENCE);

  // Group DMs (multiple recipients) are not supported as AP inbound
  if (toRecipients.length !== 1) return null;

  const now = (ctx.now ?? (() => new Date()))();
  const actorId = getActorId(activity.actor);

  // Resolve all participant refs in parallel
  const [sourceAccountRef, ...recipientRefs] = await Promise.all([
    ctx.resolveActorRef({ activityPubActorUri: actorId }),
    ...toRecipients.map((uri) => ctx.resolveActorRef({ activityPubActorUri: uri })),
  ]);

  if (!sourceAccountRef || recipientRefs.some((r) => !r)) return null;

  // Safe: we just verified every element is truthy above
  const allRecipientRefs = recipientRefs as NonNullable<(typeof recipientRefs)[number]>[];
  const primaryRecipient = allRecipientRefs[0];
  if (!primaryRecipient) return null;
  const additionalRecipients = allRecipientRefs.slice(1);

  // Participant set for mention scoping: sender + all recipients
  const participantUris = new Set<string>([
    actorId,
    ...toRecipients,
  ]);

  const objectContent = asString(activity.object?.["content"]) ?? "";
  // eslint-disable-next-line no-control-regex
  const sanitizedContent = objectContent.replace(/\x00/g, "").slice(0, MAX_TEXT_LEN);

  const { plaintext, blocks: _blocks } = htmlToCanonicalBlocks(sanitizedContent);
  const text = plaintext.slice(0, MAX_TEXT_LEN);

  const objectId = activity.object?.id;
  const messageId = objectId ?? `${actorId}#dm-${Date.now()}`;

  const attachments = buildAttachments(activity.object?.["attachment"], messageId);
  const customEmojis = collectApCustomEmojis(activity.object?.["tag"], {
    referencedText: [sanitizedContent],
    fallbackDomain: actorId,
  });

  const rawTagFacets = await buildTagFacets(text, activity.object?.["tag"], ctx);
  const inlineFacets = buildInlineLinkAndTagFacets(text);
  const mergedFacets = mergeFacets(rawTagFacets, inlineFacets);

  // Scope mentions to conversation participants; mark all hashtags as private
  const facets: CanonicalFacet[] = mergedFacets.flatMap((facet): CanonicalFacet[] => {
    if (facet.type === "tag") {
      return [{ ...facet, private: true }];
    }
    if (facet.type === "mention") {
      const uri = facet.target.activityPubActorUri;
      if (uri && participantUris.has(uri)) return [facet];
      return []; // drop mentions of non-participants
    }
    return [facet];
  });

  // Link preview: first link-type facet URL (non-blocking, silently skip on failure)
  const firstLinkUrl = facets.find((f) => f.type === "link")?.url ?? null;
  let linkPreview: CanonicalDirectMessageIntent["linkPreview"] = null;
  if (firstLinkUrl) {
    try {
      const og = await fetchOpenGraph(firstLinkUrl);
      if (og) {
        linkPreview = {
          uri: og.uri,
          title: og.title,
          description: og.description ?? null,
          thumbUrl: og.thumbUrl ?? null,
          ...(og.authorName ? { authorName: og.authorName } : {}),
          ...(og.authorUrl ? { authorUrl: og.authorUrl } : {}),
          ...(og.authors && og.authors.length > 0 ? { authors: og.authors } : {}),
        };
      }
    } catch {
      // Link preview is best-effort
    }
  }

  const inReplyTo = await resolveOptionalObjectRef(activity.object?.["inReplyTo"], ctx);

  // FEP-2931: strip "/context" suffix from context URL to derive thread root
  const rawContext = activity.object?.["context"];
  const contextId = asString(rawContext) ?? extractFirstUrl(rawContext);
  const rootId = contextId?.replace(/\/context$/, "") ?? null;
  const replyRoot = rootId
    ? await ctx.resolveObjectRef({
        canonicalObjectId: rootId,
        activityPubObjectId: /^https?:\/\//.test(rootId) ? rootId : null,
        canonicalUrl: /^https?:\/\//.test(rootId) ? rootId : null,
      })
    : inReplyTo;

  // Quote-post support: FEP-044f + Misskey compat aliases
  const rawQuote =
    activity.object?.["quote"] ??
    activity.object?.["quoteUrl"] ??
    activity.object?.["quoteUri"] ??
    activity.object?.["_misskey_quote"];
  const quoteOf = rawQuote ? await resolveOptionalObjectRef(rawQuote, ctx) : null;

  const timestamp =
    asString(activity.object?.["published"]) ?? activity.published ?? now.toISOString();

  const draft: Omit<CanonicalDirectMessageIntent, "canonicalIntentId"> = {
    kind: "DirectMessage",
    sourceProtocol: "activitypub",
    sourceEventId: activity.id,
    sourceAccountRef,
    createdAt: timestamp,
    observedAt: now.toISOString(),
    visibility: "direct",
    provenance: {
      originProtocol: "activitypub",
      originEventId: activity.id,
      originAccountId: sourceAccountRef.canonicalAccountId ?? null,
      mirroredFromCanonicalIntentId: null,
      projectionMode: "native",
    },
    warnings: [],
    sender: sourceAccountRef,
    recipient: primaryRecipient,
    additionalRecipients,
    text,
    messageId,
    timestamp,
    facets,
    attachments,
    customEmojis,
    linkPreview,
    inReplyTo,
    replyRoot,
    quoteOf,
  };

  return {
    ...draft,
    canonicalIntentId: buildCanonicalIntentId(draft),
  };
}
