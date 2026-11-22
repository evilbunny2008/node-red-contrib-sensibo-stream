'use strict';

const { SensiboAccount } = require('../lib/sensibo-account');

module.exports = function (RED) {
    function SensiboConfigNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        node.name = config.name;
        node.pollMinutes = Math.max(15, parseInt(config.pollMinutes, 10) || 15);
        node.fetchOnChange = config.fetchOnChange !== false;
        node.lookupCause = config.lookupCause === true;
        node.users = new Set();
        node.account = null;

        const apiKey = node.credentials && node.credentials.apiKey;

        function getAccount() {
            if (node.account) return node.account;
            if (!apiKey) return null;
            node.account = new SensiboAccount({
                apiKey,
                // Undocumented override, intended for testing against a mock server.
                baseUrl: process.env.SENSIBO_BASE_URL || undefined,
                pollIntervalMs: node.pollMinutes * 60 * 1000,
                fetchOnChange: node.fetchOnChange,
                lookupCause: node.lookupCause
            });
            node.account.on('warn', (err) => node.warn(err && err.message ? err.message : String(err)));
            return node.account;
        }

        // One shared stream per account (Sensibo allows max 10 open streams per account).
        node.register = function (user) {
            const account = getAccount();
            if (!account) return null;
            node.users.add(user);
            if (node.users.size === 1) account.start();
            return account;
        };

        node.deregister = function (user) {
            node.users.delete(user);
            if (node.users.size === 0 && node.account) node.account.stop();
        };

        node.on('close', function (done) {
            if (node.account) {
                node.account.stop();
                node.account.removeAllListeners();
                node.account = null;
            }
            node.users.clear();
            done();
        });
    }

    RED.nodes.registerType('sensibo-stream-account', SensiboConfigNode, {
        credentials: {
            apiKey: { type: 'password' }
        }
    });
};
