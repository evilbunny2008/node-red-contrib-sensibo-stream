'use strict';

const STATUS = {
    connecting: { fill: 'yellow', shape: 'ring' },
    connected: { fill: 'green', shape: 'dot' },
    disconnected: { fill: 'red', shape: 'ring' },
    unavailable: { fill: 'yellow', shape: 'dot' },
    error: { fill: 'red', shape: 'dot' },
    stopped: { fill: 'grey', shape: 'ring' }
};

module.exports = function (RED) {
    function SensiboStreamNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        const accountNode = RED.nodes.getNode(config.account);

        const deviceFilter = new Set(String(config.devices || '')
            .split(/[\s,;]+/).map((s) => s.trim()).filter(Boolean));
        const wanted = {
            hello: config.evHello !== false,
            measurements: config.evMeasurements !== false,
            changed: config.evChanged !== false,
            resync: config.evResync === true,
            device: config.evDevice !== false,
            poll: config.evPoll !== false
        };
        const includeDevice = config.includeDevice !== false;

        if (!accountNode) {
            node.status({ fill: 'red', shape: 'ring', text: 'no account configured' });
            return;
        }
        const account = accountNode.register(node);
        if (!account) {
            node.status({ fill: 'red', shape: 'ring', text: 'missing API key' });
            return;
        }

        function matches(...ids) {
            if (deviceFilter.size === 0) return true;
            return ids.some((id) => id && deviceFilter.has(id));
        }

        function onStatus({ state, text }) {
            node.status(Object.assign({ text }, STATUS[state] || STATUS.stopped));
        }

        function onEvent({ event, data }) {
            if (!wanted[event] && ['hello', 'measurements', 'changed', 'resync'].includes(event)) return;
            const id = data && data.id;
            const podId = data && data.podId;
            if (id && !matches(id, podId)) return;
            const msg = {
                topic: 'sensibo/' + event + (id ? '/' + id : ''),
                event,
                payload: data
            };
            if (id) msg.deviceId = id;
            if (podId) msg.podId = podId;
            if (event === 'changed' && data) msg.fields = data.fields || [];
            if (includeDevice && id) {
                const d = account.getDevice(id);
                if (d) msg.device = RED.util.cloneMessage(d);
            }
            node.send(msg);
        }

        function onDevice({ id, device, reason, fields, change }) {
            const kind = reason === 'changed' ? 'device' : 'poll';
            if (!wanted[kind]) return;
            if (!matches(id)) return;
            const msg = {
                topic: 'sensibo/' + kind + '/' + id,
                event: kind,
                reason,
                deviceId: id,
                fields: fields || [],
                payload: RED.util.cloneMessage(device)
            };
            if (change) {
                msg.change = RED.util.cloneMessage(change);
                if (change.reason) msg.cause = change.reason;
            }
            node.send(msg);
        }

        account.on('status', onStatus);
        account.on('event', onEvent);
        account.on('device', onDevice);

        // If the shared connection was already running (e.g. only this node was redeployed),
        // replay the cached device state so this node starts with all values.
        if (account.loaded) {
            setImmediate(() => {
                for (const device of account.getLoadedDevices()) {
                    onDevice({ id: device.id, device, reason: 'cached', fields: [] });
                }
            });
        }
        onStatus({ state: account.state === 'stopped' ? 'connecting' : account.state, text: account.state === 'stopped' ? 'connecting' : account.state });

        node.on('input', function (msg, send, done) {
            const cmd = typeof msg.payload === 'string' ? msg.payload.toLowerCase() : '';
            if (cmd === 'reconnect') {
                account.reconnect();
                done();
            } else {
                account.refresh('refresh').then(() => done(), done);
            }
        });

        node.on('close', function (done) {
            account.off('status', onStatus);
            account.off('event', onEvent);
            account.off('device', onDevice);
            accountNode.deregister(node);
            done();
        });
    }

    RED.nodes.registerType('sensibo-event-stream', SensiboStreamNode);
};
