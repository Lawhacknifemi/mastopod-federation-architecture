const path = require('path');
const urlJoin = require('url-join');
const { AuthLocalService } = require('@semapps/auth');
const CONFIG = require('../../config/config');
const transport = require('../../config/transport');

module.exports = {
  mixins: [AuthLocalService],
  settings: {
    baseUrl: CONFIG.BASE_URL,
    jwtPath: path.resolve(__dirname, '../../jwt'),
    accountsDataset: CONFIG.AUTH_ACCOUNTS_DATASET,
    reservedUsernames: CONFIG.AUTH_RESERVED_USER_NAMES,
    minPasswordLength: 8, // Same as frontend requirement
    minUsernameLength: 2,
    webIdSelection: ['nick', 'schema:knowsLanguage'],
    formUrl: CONFIG.FRONTEND_URL ? urlJoin(CONFIG.FRONTEND_URL, 'login') : undefined,
    podProvider: true,
    mail: {
      from: `${CONFIG.FROM_NAME} <${CONFIG.FROM_EMAIL}>`,
      transport,
      defaults: {
        locale: CONFIG.DEFAULT_LOCALE,
        frontUrl: CONFIG.FRONTEND_URL
      }
    }
  },
  hooks: {
    after: {
      async signup(ctx, res) {
        const { webId } = res;

        await ctx.call('auth-agent.waitForResourceCreation', { webId });
        await ctx.call('agent-registry.waitForResourceCreation', { webId });
        await ctx.call('auth-registry.waitForResourceCreation', { webId });
        await ctx.call('data-registry.waitForResourceCreation', { webId });

        await ctx.call('activitypub.actor.awaitCreateComplete', {
          actorUri: webId,
          additionalKeys: [
            'pim:storage',
            'pim:preferencesFile',
            'interop:hasAuthorizationAgent',
            'interop:hasRegistrySet',
            'solid:publicTypeIndex'
          ]
        });

        // Wait until all data and type registrations are created
        // This is necessary for the data provider to be able to load all containers
        await ctx.call('data-registry.awaitCreateComplete', { webId });
        await ctx.call('type-indexes.awaitCreateComplete', { webId });

        // Auto-generate DID and register mapping for ATProto
        try {
          const url = new URL(webId);
          const username = url.pathname.replace(/^\//, '').split('/')[0];
          const hostname = url.hostname;
          const handle = `${username}.${hostname}`;

          ctx.broker.logger.info(`[Auth] Auto-creating ATProto DID for signup: ${handle}`);

          const didResult = await ctx.call('atproto.identity.createDid', {
            handle,
            pdsServer: CONFIG.BASE_URL,
            displayName: username,
            description: `ATProto bridge account for ${webId}`
          });

          if (didResult && didResult.did) {
            await ctx.call('atproto.bridge.registerMapping', {
              actorUri: webId,
              did: didResult.did
            });
            ctx.broker.logger.info(`[Auth] Registered ATProto mapping: ${webId} -> ${didResult.did}`);
          }
        } catch (error) {
          ctx.broker.logger.error('[Auth] Failed to auto-create ATProto identity:', error.message);
        }

        return res;
      }
    }
  }
};
