/**
 * V6.5 Phase 5.5: Unified OpenSearch Read Stack
 *
 * OpenSearch mapping for public-content-v1
 */

export const PublicContentMapping = {
  settings: {
    index: {
      knn: true,
      number_of_shards: 3,
      number_of_replicas: 1,
      default_pipeline: "public-content-ingest-v1"
    },
    analysis: {
      analyzer: {
        content_text_analyzer: {
          type: "custom",
          tokenizer: "standard",
          filter: ["lowercase"]
        }
      }
    }
  },
  mappings: {
    dynamic: "strict",
    properties: {
      stableDocId:        { type: 'keyword' },
      canonicalContentId: { type: 'keyword' },

      protocolPresence:   { type: 'keyword' },
      sourceKind:         { type: 'keyword' },

      ap: {
        properties: {
          objectUri: { type: 'keyword' },
          activityUri: { type: 'keyword' }
        }
      },

      at: {
        properties: {
          uri: { type: 'keyword' },
          cid: { type: 'keyword' },
          did: { type: 'keyword' }
        }
      },

      author: {
        properties: {
          canonicalId: { type: 'keyword' },
          apUri: { type: 'keyword' },
          did: { type: 'keyword' },
          handle: { type: 'keyword' },
          displayName: {
            type: 'text',
            analyzer: 'content_text_analyzer',
            fields: {
              raw: {
                type: 'keyword',
                ignore_above: 256
              }
            }
          }
        }
      },

      text: {
        type: 'text',
        analyzer: 'content_text_analyzer',
        fields: {
          raw: {
            type: 'keyword',
            ignore_above: 32766
          }
        }
      },
      textRaw: {
        type: 'keyword',
        index: false,
        doc_values: false
      },

      createdAt: {
        type: 'date',
        format: 'strict_date_optional_time||epoch_millis'
      },
      updatedAt: {
        type: 'date',
        format: 'strict_date_optional_time||epoch_millis'
      },
      indexedAt: {
        type: 'date',
        format: 'strict_date_optional_time||epoch_millis'
      },

      langs: { type: 'keyword' },
      tags: { type: 'keyword' },

      replyToStableId: { type: 'keyword' },
      quoteOfStableId: { type: 'keyword' },

      hasMedia: { type: 'boolean' },
      mediaCount: { type: 'integer' },

      engagement: {
        properties: {
          likeCount: { type: 'integer' },
          repostCount: { type: 'integer' },
          replyCount: { type: 'integer' }
        }
      },

      ranking: {
        properties: {
          recencyBucket: { type: 'keyword' },
          localAffinityScore: { type: 'float' },
          graphAffinityScore: { type: 'float' },
          qualityScore: { type: 'float' }
        }
      },

      embedding: {
        type: 'knn_vector',
        dimension: 1024,
        method: {
          name: 'hnsw',
          space_type: 'cosinesimil',
          engine: 'nmslib'
        }
      },
      embeddingStatus: { type: 'keyword' },
      embeddingUpdatedAt: {
        type: 'date',
        format: 'strict_date_optional_time||epoch_millis'
      },

      isDeleted: { type: 'boolean' }
    }
  }
};
