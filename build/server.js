var $dDqCW$process = require("process");
var $dDqCW$book = require("book");
var $dDqCW$koa = require("koa");
var $dDqCW$tldjs = require("tldjs");
var $dDqCW$debug = require("debug");
var $dDqCW$http = require("http");
var $dDqCW$humanreadableids = require("human-readable-ids");
var $dDqCW$koarouter = require("koa-router");
var $dDqCW$pump = require("pump");
var $dDqCW$events = require("events");
var $dDqCW$net = require("net");
require("assert");

function $parcel$interopDefault(a) {
  return a && a.__esModule ? a.default : a;
}
function $parcel$defineInteropFlag(a) {
  Object.defineProperty(a, '__esModule', {value: true, configurable: true});
}
function $parcel$export(e, n, v, s) {
  Object.defineProperty(e, n, {get: v, set: s, enumerable: true, configurable: true});
}

$parcel$defineInteropFlag(module.exports);

$parcel$export(module.exports, "default", () => $2685e5b20c9f29f6$export$2e2bcd8739ae039);













// A client encapsulates req/res handling using an agent
//
// If an agent is destroyed, the request handling will error
// The caller is responsible for handling a failed request
class $dd3f459a68264663$var$Client extends (0, ($parcel$interopDefault($dDqCW$events))) {
    constructor(options){
        super();
        const agent = this.agent = options.agent;
        const id = this.id = options.id;
        this.debug = (0, ($parcel$interopDefault($dDqCW$debug)))(`lt:Client[${this.id}]`);
        // client is given a grace period in which they can connect before they are _removed_
        this.graceTimeout = setTimeout(()=>{
            this.close();
        }, 1000).unref();
        agent.on("online", ()=>{
            this.debug("client online %s", id);
            clearTimeout(this.graceTimeout);
        });
        agent.on("offline", ()=>{
            this.debug("client offline %s", id);
            // if there was a previous timeout set, we don't want to double trigger
            clearTimeout(this.graceTimeout);
            // client is given a grace period in which they can re-connect before they are _removed_
            this.graceTimeout = setTimeout(()=>{
                this.close();
            }, 1000).unref();
        });
        // TODO(roman): an agent error removes the client, the user needs to re-connect?
        // how does a user realize they need to re-connect vs some random client being assigned same port?
        agent.once("error", (err)=>{
            this.close();
        });
    }
    stats() {
        return this.agent.stats();
    }
    close() {
        clearTimeout(this.graceTimeout);
        this.agent.destroy();
        this.emit("close");
    }
    handleRequest(req, res) {
        this.debug("> %s", req.url);
        const opt = {
            path: req.url,
            agent: this.agent,
            method: req.method,
            headers: req.headers
        };
        const clientReq = (0, ($parcel$interopDefault($dDqCW$http))).request(opt, (clientRes)=>{
            this.debug("< %s", req.url);
            // write response code and headers
            res.writeHead(clientRes.statusCode, clientRes.headers);
            // using pump is deliberate - see the pump docs for why
            (0, ($parcel$interopDefault($dDqCW$pump)))(clientRes, res);
        });
        // this can happen when underlying agent produces an error
        // in our case we 504 gateway error this?
        // if we have already sent headers?
        clientReq.once("error", (err)=>{
        // TODO(roman): if headers not sent - respond with gateway unavailable
        });
        // using pump is deliberate - see the pump docs for why
        (0, ($parcel$interopDefault($dDqCW$pump)))(req, clientReq);
    }
    handleUpgrade(req, socket) {
        this.debug("> [up] %s", req.url);
        socket.once("error", (err)=>{
            // These client side errors can happen if the client dies while we are reading
            // We don't need to surface these in our logs.
            if (err.code == "ECONNRESET" || err.code == "ETIMEDOUT") return;
            console.error(err);
        });
        this.agent.createConnection({}, (err, conn)=>{
            this.debug("< [up] %s", req.url);
            // any errors getting a connection mean we cannot service this request
            if (err) {
                socket.end();
                return;
            }
            // socket met have disconnected while we waiting for a socket
            if (!socket.readable || !socket.writable) {
                conn.destroy();
                socket.end();
                return;
            }
            // websocket requests are special in that we simply re-create the header info
            // then directly pipe the socket data
            // avoids having to rebuild the request and handle upgrades via the http client
            const arr = [
                `${req.method} ${req.url} HTTP/${req.httpVersion}`
            ];
            for(let i = 0; i < req.rawHeaders.length - 1; i += 2)arr.push(`${req.rawHeaders[i]}: ${req.rawHeaders[i + 1]}`);
            arr.push("");
            arr.push("");
            // using pump is deliberate - see the pump docs for why
            (0, ($parcel$interopDefault($dDqCW$pump)))(conn, socket);
            (0, ($parcel$interopDefault($dDqCW$pump)))(socket, conn);
            conn.write(arr.join("\r\n"));
        });
    }
}
var $dd3f459a68264663$export$2e2bcd8739ae039 = $dd3f459a68264663$var$Client;







const $ed3d3cfb73a7a594$var$DEFAULT_MAX_SOCKETS = 10;
// Implements an http.Agent interface to a pool of tunnel sockets
// A tunnel socket is a connection _from_ a client that will
// service http requests. This agent is usable wherever one can use an http.Agent
class $ed3d3cfb73a7a594$var$TunnelAgent extends (0, $dDqCW$http.Agent) {
    constructor(options = {}){
        super({
            keepAlive: true,
            // only allow keepalive to hold on to one socket
            // this prevents it from holding on to all the sockets so they can be used for upgrades
            maxFreeSockets: 1
        });
        // sockets we can hand out via createConnection
        this.availableSockets = [];
        // when a createConnection cannot return a socket, it goes into a queue
        // once a socket is available it is handed out to the next callback
        this.waitingCreateConn = [];
        this.debug = (0, ($parcel$interopDefault($dDqCW$debug)))(`lt:TunnelAgent[${options.clientId}]`);
        // track maximum allowed sockets
        this.connectedSockets = 0;
        this.maxTcpSockets = options.maxTcpSockets || $ed3d3cfb73a7a594$var$DEFAULT_MAX_SOCKETS;
        // new tcp server to service requests for this client
        this.server = (0, ($parcel$interopDefault($dDqCW$net))).createServer();
        // flag to avoid double starts
        this.started = false;
        this.closed = false;
    }
    stats() {
        return {
            connectedSockets: this.connectedSockets
        };
    }
    listen() {
        const server = this.server;
        if (this.started) throw new Error("already started");
        this.started = true;
        server.on("close", this._onClose.bind(this));
        server.on("connection", this._onConnection.bind(this));
        server.on("error", (err)=>{
            // These errors happen from killed connections, we don't worry about them
            if (err.code == "ECONNRESET" || err.code == "ETIMEDOUT") return;
            (0, ($parcel$interopDefault($dDqCW$book))).error(err);
        });
        return new Promise((resolve)=>{
            server.listen(()=>{
                const port = server.address().port;
                this.debug("tcp server listening on port: %d", port);
                resolve({
                    // port for lt client tcp connections
                    port: port
                });
            });
        });
    }
    _onClose() {
        this.closed = true;
        this.debug("closed tcp socket");
        // flush any waiting connections
        for (const conn of this.waitingCreateConn)conn(new Error("closed"), null);
        this.waitingCreateConn = [];
        this.emit("end");
    }
    // new socket connection from client for tunneling requests to client
    _onConnection(socket) {
        // no more socket connections allowed
        if (this.connectedSockets >= this.maxTcpSockets) {
            this.debug("no more sockets allowed");
            socket.destroy();
            return false;
        }
        socket.once("close", (hadError)=>{
            this.debug("closed socket (error: %s)", hadError);
            this.connectedSockets -= 1;
            // remove the socket from available list
            const idx = this.availableSockets.indexOf(socket);
            if (idx >= 0) this.availableSockets.splice(idx, 1);
            this.debug("connected sockets: %s", this.connectedSockets);
            if (this.connectedSockets <= 0) {
                this.debug("all sockets disconnected");
                this.emit("offline");
            }
        });
        // close will be emitted after this
        socket.once("error", (err)=>{
            // we do not log these errors, sessions can drop from clients for many reasons
            // these are not actionable errors for our server
            socket.destroy();
        });
        if (this.connectedSockets === 0) this.emit("online");
        this.connectedSockets += 1;
        this.debug("new connection from: %s:%s", socket.address().address, socket.address().port);
        // if there are queued callbacks, give this socket now and don't queue into available
        const fn = this.waitingCreateConn.shift();
        if (fn) {
            this.debug("giving socket to queued conn request");
            setTimeout(()=>{
                fn(null, socket);
            }, 0);
            return;
        }
        // make socket available for those waiting on sockets
        this.availableSockets.push(socket);
    }
    // fetch a socket from the available socket pool for the agent
    // if no socket is available, queue
    // cb(err, socket)
    createConnection(options, cb) {
        if (this.closed) {
            cb(new Error("closed"));
            return;
        }
        this.debug("create connection");
        // socket is a tcp connection back to the user hosting the site
        const sock = this.availableSockets.shift();
        // no available sockets
        // wait until we have one
        if (!sock) {
            this.waitingCreateConn.push(cb);
            this.debug("waiting connected: %s", this.connectedSockets);
            this.debug("waiting available: %s", this.availableSockets.length);
            return;
        }
        this.debug("socket given");
        cb(null, sock);
    }
    destroy() {
        this.server.close();
        super.destroy();
    }
}
var $ed3d3cfb73a7a594$export$2e2bcd8739ae039 = $ed3d3cfb73a7a594$var$TunnelAgent;


// Manage sets of clients
//
// A client is a "user session" established to service a remote localtunnel client
class $adc79b28c1d4c625$var$ClientManager {
    constructor(opt){
        this.opt = opt || {};
        // id -> client instance
        this.clients = new Map();
        // statistics
        this.stats = {
            tunnels: 0
        };
        this.debug = (0, ($parcel$interopDefault($dDqCW$debug)))("lt:ClientManager");
        // This is totally wrong :facepalm: this needs to be per-client...
        this.graceTimeout = null;
    }
    // create a new tunnel with `id`
    // if the id is already used, a random id is assigned
    // if the tunnel could not be created, throws an error
    async newClient(id) {
        const clients = this.clients;
        const stats = this.stats;
        // can't ask for id already is use
        if (clients[id]) id = (0, $dDqCW$humanreadableids.hri).random();
        const maxSockets = this.opt.max_tcp_sockets;
        const agent = new (0, $ed3d3cfb73a7a594$export$2e2bcd8739ae039)({
            clientId: id,
            maxSockets: 10
        });
        const client = new (0, $dd3f459a68264663$export$2e2bcd8739ae039)({
            id: id,
            agent: agent
        });
        // add to clients map immediately
        // avoiding races with other clients requesting same id
        clients[id] = client;
        client.once("close", ()=>{
            this.removeClient(id);
        });
        // try/catch used here to remove client id
        try {
            const info = await agent.listen();
            ++stats.tunnels;
            return {
                id: id,
                port: info.port,
                max_conn_count: maxSockets
            };
        } catch (err) {
            this.removeClient(id);
            // rethrow error for upstream to handle
            throw err;
        }
    }
    removeClient(id) {
        this.debug("removing client: %s", id);
        const client = this.clients[id];
        if (!client) return;
        --this.stats.tunnels;
        delete this.clients[id];
        client.close();
    }
    hasClient(id) {
        return !!this.clients[id];
    }
    getClient(id) {
        return this.clients[id];
    }
}
var $adc79b28c1d4c625$export$2e2bcd8739ae039 = $adc79b28c1d4c625$var$ClientManager;



const $2685e5b20c9f29f6$var$debug = (0, ($parcel$interopDefault($dDqCW$debug)))("localtunnel:server");
function $2685e5b20c9f29f6$export$2e2bcd8739ae039(opt) {
    opt = opt || {};
    const validHosts = opt.domain ? [
        opt.domain
    ] : undefined;
    const myTldjs = (0, ($parcel$interopDefault($dDqCW$tldjs))).fromUserSettings({
        validHosts: validHosts
    });
    const landingPage = opt.landing || "https://localtunnel.github.io/www/";
    function GetClientIdFromHostname(hostname) {
        return myTldjs.getSubdomain(hostname);
    }
    const manager = new (0, $adc79b28c1d4c625$export$2e2bcd8739ae039)(opt);
    const schema = opt.secure ? "https" : "http";
    const app = new (0, ($parcel$interopDefault($dDqCW$koa)))();
    const router = new (0, ($parcel$interopDefault($dDqCW$koarouter)))();
    router.get("/api/status", async (ctx, next)=>{
        const stats = manager.stats;
        ctx.body = {
            tunnels: stats.tunnels,
            mem: $dDqCW$process.memoryUsage()
        };
    });
    router.get("/api/tunnels/:id/status", async (ctx, next)=>{
        const clientId = ctx.params.id;
        const client = manager.getClient(clientId);
        if (!client) {
            ctx.throw(404);
            return;
        }
        const stats = client.stats();
        ctx.body = {
            connected_sockets: stats.connectedSockets
        };
    });
    app.use(router.routes());
    app.use(router.allowedMethods());
    // root endpoint
    app.use(async (ctx, next)=>{
        const path = ctx.request.path;
        // skip anything not on the root path
        if (path !== "/") {
            await next();
            return;
        }
        const isNewClientRequest = ctx.query["new"] !== undefined;
        if (isNewClientRequest) {
            const reqId = (0, $dDqCW$humanreadableids.hri).random();
            $2685e5b20c9f29f6$var$debug("making new client with id %s", reqId);
            const info = await manager.newClient(reqId);
            const url = schema + "://" + info.id + "." + ctx.request.host;
            info.url = url;
            if (opt.ip) {
                console.log("IP ASSIGNED", info.ip);
                info.ip = opt.ip;
            } else console.log("NO IP ASSIGNED", opt);
            ctx.body = info;
            return;
        }
        // no new client request, send to landing page
        ctx.redirect(landingPage);
    });
    // anything after the / path is a request for a specific client name
    // This is a backwards compat feature
    app.use(async (ctx, next)=>{
        console.log("KOA APP REQUEST HAPPENING", ctx.request.path);
        const parts = ctx.request.path.split("/");
        // any request with several layers of paths is not allowed
        // rejects /foo/bar
        // allow /foo
        if (parts.length !== 2) {
            console.log("SKIPPING", parts.length);
            await next();
            return;
        }
        const reqId = parts[1];
        console.log("REQ ID", reqId);
        // limit requested hostnames to 63 characters
        if (!/^(?:[a-z0-9][a-z0-9\-]{4,63}[a-z0-9]|[a-z0-9]{4,63})$/.test(reqId)) {
            const msg = "Invalid subdomain. Subdomains must be lowercase and between 4 and 63 alphanumeric characters.";
            ctx.status = 403;
            ctx.body = {
                message: msg
            };
            return;
        }
        $2685e5b20c9f29f6$var$debug("making new client with id %s", reqId);
        const info = await manager.newClient(reqId);
        console.log("MADE NEW CLIENT", {
            info: info,
            host: ctx.request.host
        });
        const url = schema + "://" + info.id + "." + ctx.request.host;
        info.url = url;
        ctx.body = info;
        return;
    });
    if (opt.server) console.log("USING PROVIDED SERVER", opt);
    else console.log("CREATING DEFAULT HTTP SERVER");
    const server = opt.server || (0, ($parcel$interopDefault($dDqCW$http))).createServer();
    const appCallback = app.callback();
    server.on("request", (req, res)=>{
        // without a hostname, we won't know who the request is for
        const hostname = req.headers.host;
        if (!hostname) {
            res.statusCode = 400;
            res.end("Host header is required");
            return;
        }
        const clientId = GetClientIdFromHostname(hostname);
        if (!clientId) {
            console.log("NO CLIENT ID FROM HOSTNAME?", {
                hostname: hostname,
                clientId: clientId
            });
            appCallback(req, res);
            return;
        }
        const client = manager.getClient(clientId);
        if (!client) {
            console.log("NO CLIENT", {
                clientId: clientId,
                client: client,
                manager: manager
            });
            res.statusCode = 404;
            res.end("404 NO CLIENT YO");
            return;
        }
        client.handleRequest(req, res);
    });
    server.on("upgrade", (req, socket, head)=>{
        const hostname = req.headers.host;
        if (!hostname) {
            socket.destroy();
            return;
        }
        const clientId = GetClientIdFromHostname(hostname);
        if (!clientId) {
            socket.destroy();
            return;
        }
        const client = manager.getClient(clientId);
        if (!client) {
            socket.destroy();
            return;
        }
        client.handleUpgrade(req, socket);
    });
    return server;
}


//# sourceMappingURL=server.js.map
