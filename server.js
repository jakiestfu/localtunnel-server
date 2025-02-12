import log from 'book';
import Koa from 'koa';
import tldjs from 'tldjs';
import Debug from 'debug';
import http from 'http';
import { hri } from 'human-readable-ids';
import Router from 'koa-router';

import ClientManager from './lib/ClientManager';

const debug = Debug('localtunnel:server');

export default function(opt) {
    opt = opt || {};

    const validHosts = (opt.domain) ? [opt.domain] : undefined;
    const myTldjs = tldjs.fromUserSettings({ validHosts });
    const landingPage = opt.landing || 'https://localtunnel.github.io/www/';

    function GetClientIdFromHostname(hostname) {
        return myTldjs.getSubdomain(hostname);
    }

    const manager = new ClientManager(opt);

    const schema = opt.secure ? 'https' : 'http';

    const app = new Koa();
    const router = new Router();

    router.get('/api/status', async (ctx, next) => {
        const stats = manager.stats;
        ctx.body = {
            tunnels: stats.tunnels,
            mem: process.memoryUsage(),
        };
    });

    router.get('/api/tunnels/:id/status', async (ctx, next) => {
        const clientId = ctx.params.id;
        const client = manager.getClient(clientId);
        if (!client) {
            ctx.throw(404);
            return;
        }

        const stats = client.stats();
        ctx.body = {
            connected_sockets: stats.connectedSockets,
        };
    });

    app.use(router.routes());
    app.use(router.allowedMethods());

    // root endpoint
    app.use(async (ctx, next) => {
        const path = ctx.request.path;

        // skip anything not on the root path
        if (path !== '/') {
            await next();
            return;
        }

        const isNewClientRequest = ctx.query['new'] !== undefined;
        if (isNewClientRequest) {
            const reqId = hri.random();
            debug('making new client with id %s', reqId);
            const info = await manager.newClient(reqId);

            const url = schema + '://' + info.id + '.' + opt.domain || ctx.request.host;
            info.url = url;
            if (opt.ip) {
                info.ip = opt.ip;
                debug('IP ASSIGNED %O', info)
            } else {
               debug('NO IP ASSIGNED %O', opt)
            }
            debug('INFO %O', info)
            ctx.body = info;
            return;
        }

        // no new client request, send to landing page
        ctx.redirect(landingPage);
    });

    // anything after the / path is a request for a specific client name
    // This is a backwards compat feature
    app.use(async (ctx, next) => {
       debug('KOA APP REQUEST HAPPENING %s', ctx.request.path)
        const parts = ctx.request.path.split('/');

        // any request with several layers of paths is not allowed
        // rejects /foo/bar
        // allow /foo
        if (parts.length !== 2) {
           debug('SKIPPING %O', parts)
            await next();
            return;
        }

        const reqId = parts[1];
        debug('REQ ID', reqId)
        // limit requested hostnames to 63 characters
        if (! /^(?:[a-z0-9][a-z0-9\-]{4,63}[a-z0-9]|[a-z0-9]{4,63})$/.test(reqId)) {
            const msg = 'Invalid subdomain. Subdomains must be lowercase and between 4 and 63 alphanumeric characters.';
            ctx.status = 403;
            ctx.body = {
                message: msg,
            };
            return;
        }

        debug('making new client with id %s', reqId);
        const info = await manager.newClient(reqId);
       debug('MADE NEW CLIENT %O', {info, host: ctx.request.host})
        // const url = schema + '://' + info.id + '.' + ctx.request.host;
        const url = schema + '://' + info.id + '.' + opt.domain || ctx.request.host;
        info.url = url;
        info.ip = opt.ip
        ctx.body = info;
        return;
    });

    if (opt.server) {
       debug('USING PROVIDED SERVER %O', opt)
    } else {
       debug('CREATING DEFAULT HTTP SERVER')
    }
    const server = opt.server || http.createServer();

    const appCallback = app.callback();

    server.on('request', (req, res) => {
        debug('SERVER REQUEST %O', req.headers)
        // without a hostname, we won't know who the request is for
        const hostname = req.headers.host;
        if (!hostname) {
            res.statusCode = 400;
            res.end('Host header is required');
            return;
        }

        const clientId = GetClientIdFromHostname(hostname);
        if (!clientId) {
           debug('NO CLIENT ID FROM HOSTNAME? %O', {hostname, clientId})
            appCallback(req, res);
            return;
        }

        const client = manager.getClient(clientId);
        if (!client) {
           debug('NO CLIENT %O', {clientId, client, manager})
            res.statusCode = 404;
            res.end('404 NO CLIENT YO');
            return;
        }

        client.handleRequest(req, res);
    });

    server.on('upgrade', (req, socket, head) => {
        const hostname = req.headers.host;
        if (!hostname) {
            debug('DESTROYING SOCKET - MISSING HOSTNAME %O', req.headers)
            socket.destroy();
            return;
        }

        const clientId = GetClientIdFromHostname(hostname);
        if (!clientId) {
            debug('DESTROYING SOCKET - MISSING CLIENT ID %s', clientId)
            socket.destroy();
            return;
        }

        const client = manager.getClient(clientId);
        if (!client) {
            debug('DESTROYING SOCKET - MISSING CLIENT %O', client)
            socket.destroy();
            return;
        }
        debug('UPGRADING CLIENT?')
        client.handleUpgrade(req, socket);
    });

    return server;
};
