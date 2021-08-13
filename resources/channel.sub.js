(function() {
    function randInt(bits) {
        if (bits < 1 || bits > 53) {
            throw new TypeError();
        } else {
            if (bits >= 1 && bits <= 30) {
                return 0 | ((1 << bits) * Math.random());
            } else {
                var high = (0 | ((1 << (bits - 30)) * Math.random())) * (1 << 30);
                var low = 0 | ((1 << 30) * Math.random());
                return  high + low;
            }
        }
    }


    function toHex(x, length) {
        var rv = x.toString(16);
        while (rv.length < length) {
            rv = "0" + rv;
        }
        return rv;
    }

    function createUuid() {
        return [toHex(randInt(32), 8),
         toHex(randInt(16), 4),
         toHex(0x4000 | randInt(12), 4),
         toHex(0x8000 | randInt(14), 4),
         toHex(randInt(48), 12)].join("-");
    }


    // TODO: should be a way to clean up unused sockets
    let socketCache = {read: new Map(), write: new Map()};

    async function getSocket(uuid, direction) {
        function createSocket(uuid, direction) {
            let protocol = self.isSecureContext ? "wss" : "ws";
            let port = self.isSecureContext? "{{ports[wss][0]}}" : "{{ports[ws][0]}}";
            let url = `${protocol}://{{host}}:${port}/msg_channel?uuid=${uuid}&direction=${direction}`;
            let socket = new WebSocket(url);
            console.log(socket);
            return new Promise(resolve => socket.addEventListener("open", () => resolve(socket)));
        }
        let cache = socketCache[direction];
        if (!cache) {
            throw new Error(`Unknown direction ${direction}`);
        }
        let existing = cache.get(uuid);
        if (existing && direction == "read") {
            throw new Error("Can't create multiple read sockets with same UUID");
        }
        let socket;
        if (!existing) {
            socket = await createSocket(uuid, direction);
            cache.set(uuid, socket);
        } else {
            socket = existing;
        }
        return socket;
    }

    class SendChannel {
        constructor(uuid) {
            this.uuid = uuid;
            this.socket = null;
        }

        async connect() {
            if (this.socket !== null) {
                return;
            }
            this.socket = await getSocket(this.uuid, "write");
        }

        async send(msg) {
            if (this.socket === null) {
                await this.connect();
            }
            console.log("send", msg);
            this.socket.send(JSON.stringify(msg));
        }
    };
    self.SendChannel = SendChannel;

    const recvChannelsCreated = new Set();

    class RecvChannel {
        constructor(uuid) {
            if (recvChannelsCreated.has(uuid)) {
                throw new Error(`Already created RecvChannel with id ${uuid}`);
            }
            this.uuid = uuid;
            this.socket = null;
            // Sets iterate in insertion order
            this.eventListeners = new Set();
        };

        async connect() {
            if (this.socket !== null) {
                throw new Error("Tried to connect to already connected channel");
            }
            this.socket = await getSocket(this.uuid, "read");
            this.socket.onmessage = event => this.readMessage(event.data);
        }

        readMessage(data) {
            let msg = JSON.parse(data);
            console.log("readMessage", this, msg);
            this.eventListeners.forEach(fn => fn(msg));
        };

        addEventListener(fn) {
            console.log("addEventListener", fn);
            this.eventListeners.add(fn);
        };

        removeEventListener(fn) {
            console.log("removeEventListener", fn);
            this.eventListeners.delete(fn);
        };

        next() {
            return new Promise(resolve => {
                let fn = (msg) => {
                    this.removeEventListener(fn);
                    resolve(msg);
                };
                this.addEventListener(fn);
            });
        }
    }

    self.channel = async function() {
        let uuid = createUuid();
        let recvChannel = new RecvChannel(uuid);
        let sendChannel = new SendChannel(uuid);
        await Promise.all([recvChannel.connect(), sendChannel.connect()]);
        return [recvChannel, sendChannel];
    };

    self.ctx_channel = async function() {
        let uuid = self.test_driver_internal._get_context_id(self);
        let channel = new RecvChannel(uuid);
        await channel.connect();
        return new TestDriverCommandRecvChannel(channel);
    };

    class TestDriverCommandRecvChannel {
        constructor(recvChannel) {
            this.channel = recvChannel;
            this.channel.addEventListener(this.handleMessage);
            this.messageHandlers = new Set();
        };

        async handleMessage(msg) {
            const {id, command, params, respChannel} = msg;
            let result;
            console.log(command, id, params);
            if (command === "executeScript") {
                const fnString = params.fn.value;
                const args = params.args.map(x => deserialize(x).toLocal());
                const body = `let result = (${fnString}).apply(null, args);
Promise.resolve(result).then(callback);`;
                console.log("executeScript", body);
                let value = new Promise(resolve => {
                    const fn = new Function("args", "callback", body);
                    fn(args, resolve);
                });
                result = serialize(await value);
            } else if (command === "postMessage") {
                this.messageHandlers.forEach(fn => fn(params));
                result = {};
            }
            console.log("result", result);
            let chan = deserialize(respChannel);
            console.log(chan);
            await chan.connect();
            await chan.send({id, result});
        }

        addMessageHandler(fn) {
            this.messageHandlers.add(fn);
        }

        removeMessageHandler(fn) {
            this.messageHandlers.delete(fn);
        }

        nextMessage() {
            return new Promise(resolve => {
                let fn = (msg) => {
                    this.removeEventListener(fn);
                    resolve(msg);
                };
                this.addEventListener(fn);
            });
        }
    }

    class TestDriverResponseRecvChannel {
        constructor(recvChannel) {
            this.channel = recvChannel;
            this.channel.addEventListener(msg => this.handleMessage(msg));
            this.responseHandlers = new Map();
        }

        setResponseHandler(commandId, fn) {
            this.responseHandlers.set(commandId, fn);
        }

        handleMessage(msg) {
            let {id, result} = msg;
            let handler = this.responseHandlers.get(id);
            if (handler) {
                this.responseHandlers.delete(id);
                handler(result);
            }
        }
    }

    class TestDriverRemote {
        constructor(dest) {
            if (typeof dest == "string") {
                this.sendChannel = new SendChannel(dest);
            } else {
                this.sendChannel = dest;
            }
            this.recvChannel = null;
            this.respChannel = null;
            this.connected = false;
            this.commandId = 0;
        }

        async connect() {
            if (this.connected) {
                return;
            }
            let [recvChannel, respChannel] = await self.channel();
            this.recvChannel = new TestDriverResponseRecvChannel(recvChannel);
            this.respChannel = respChannel;
            this.connected = true;
        }

        async sendMessage(command, params) {
            if (!this.connected) {
                await this.connect();
            }
            let msg = {id: this.commandId++, command, params, respChannel: serialize(this.respChannel)};
            let response = new Promise(resolve =>
                this.recvChannel.setResponseHandler(msg.id, resolve));
            this.sendChannel.send(msg);
            return await response;
        }

        async executeScript(fn, ...args) {
            let resp = await this.sendMessage("executeScript", {fn: serialize(fn), args: args.map(x => serialize(x))});
            return deserialize(resp);
        }

        postMessage(msg) {
            return this.sendMessage("postMessage", {msg});
        }
    }

    self.TestDriverRemote = TestDriverRemote;

    let remoteObjects = new Map();
    let remoteObjectById = new Map();

    function remoteId(obj) {
        let rv;
        if (remoteObjects.has(obj)) {
            rv = remoteObjects.get(obj);
        } else {
            rv = createUuid();
            remoteObjects.set(obj, rv);
            remoteObjectById.set(rv, obj);
        }
        return rv;
    }

    class RemoteObject {
        constructor(type, value, objectId) {
            this.type = type;
            this.value = value;
            this.objectId = objectId;
        }

        toLocal() {
            let toLocalInner = (x) => x instanceof RemoteObject ? x.toLocal() : x;

            switch(this.type) {
            case "function":
                return new Function(this.value);
            case "array":
                return this.value.map(toLocalInner);
            case "set": {
                let rv = new Set();
                this.value.forEach(x => rv.add(toLocalInner(x)));
                return rv;
            }
            case "object": {
                let rv = {};
                for (let [key, value] of Object.entries(this.value)) {
                    rv[key] = toLocalInner(value);
                }
                return rv;
            }
            case "map": {
                let rv = new Map();
                for (let [key, value] of this.value.entries()) {
                    rv.set(key, toLocalInner(value));
                }
                return rv;
            }
            default:
                throw new TypeError(`Can't convert remote value type ${this.type} to a local value`);
            }
        }
    }

    function serialize(obj) {
        const stack = [{item: obj}];
        let serialized = null;

        while (stack.length > 0) {
            const {item, target, targetKey} = stack.shift();
            // We override this later for non-primitives
            let type = typeof item;
            let objectId;
            let value;

            console.log(item, target, targetKey, type);

            // The handling of cross-global objects here is broken

            if (item instanceof RemoteObject) {
                type = item.type;
                objectId = item.objectId;
                value = item.value;
            } else {
                switch (type) {
                case "undefined":
                case "null":
                    break;
                case "string":
                case "boolean":
                    value = item;
                    break;
                case "number":
                    if (item !== item) {
                        value = "NaN";
                    } else if (item === 0 && 1/item == Number.NEGATIVE_INFINITY) {
                        value = "-0";
                    } else if (item === Number.POSITIVE_INFINITY) {
                        value = "+Infinity";
                    } else if (item === Number.NEGATIVE_INFINITY) {
                        value = "-Infinity";
                    } else {
                        value = item;
                    }
                    break;
                case "bigint":
                    value = obj.toString();
                    break;
                case "symbol":
                    objectId = remoteId(item);
                    break;
                default:
                    // TODO: Handle platform objects better
                    if (item instanceof RecvChannel) {
                        throw new TypeError("Can't send a RecvChannel");
                    }
                    objectId = remoteId(item);

                    if (item instanceof SendChannel) {
                        type = "sendchannel";
                        value = item.uuid;
                    } else if (Array.isArray(item)) {
                        type = "array";
                        value = [];
                        for (let child of item) {
                            stack.push({item: child, target: value});
                        }
                    } else if (item.constructor.name === "RegExp") {
                        type = "regexp";
                        let pattern = item.source;
                        let flags = item.flags;
                        value = `/{pattern}/{flags}`;
                    } else if (item.constructor.name === "Date") {
                        type = "date";
                        value = Date.prototype.toDateString.call(item);
                    } else if (item.constructor.name === "Map") {
                        type = "map";
                        value = {};
                        for (let [targetKey, child] of item.entries()) {
                            stack.push({item: child, target: value, targetKey});
                        }
                    } else if (item.constructor.name === "Set") {
                        type = "set";
                        value = [];
                        for (let child of item.entries()) {
                            stack.push([{item: child, target: value}]);
                        }
                    } else if (item.constructor.name === "WeakMap") {
                        type = "weakmap";
                    } else if (item.constructor.name === "WeakSet") {
                        type = "weakset";
                    } else if (item instanceof Error) {
                        type = "error";
                    } else if (Promise.resolve(item) === item) {
                        type = "promise";
                    } else if (item instanceof Object.getPrototypeOf(Uint8Array)) {
                        type = "typedarray";
                    } else if (item instanceof ArrayBuffer) {
                        type = "arraybuffer";
                    } else if (type === "function") {
                        value = item.toString();
                    } else {
                        // Treat as a generic object
                        value = {};
                        for (let [targetKey, child] of Object.entries(item)) {
                            stack.push({item: child, target: value, targetKey});
                        }
                    }
                }
            };

            let result = {type};
            if (objectId !== undefined) {
                result.objectId = objectId;
            }
            if (value !== undefined) {
                result.value = value;
            }
            if (target == null) {
                if (serialized !== null) {
                    throw new Error("Tried to create multiple output values");
                }
                serialized = result;
            } else {
                if (Array.isArray(target)) {
                    target.push(result);
                } else {
                    target[targetKey] = target;
                }
            }
        }
        console.log("Serialize", obj, serialized);
        return serialized;
    }

    function deserialize(obj) {
        console.log("deserialize", obj);
        let deserialized = null;
        let stack = [{item: obj, target: null}];

        while (stack.length > 0) {
            const {item, target, targetKey} = stack.shift();
            const {type, value, objectId} = item;
            let result;
            switch(type) {
            case "undefined":
                result = undefined;
                break;
            case "null":
                result = null;
                break;
            case "string":
            case "boolean":
                result = value;
                break;
            case "number":
                if (typeof value === "string") {
                    switch(value) {
                    case "NaN":
                        result = NaN;
                        break;
                    case "-0":
                        result = -0;
                        break;
                    case "+Infinity":
                        result = Number.POSITIVE_INFINITY;
                        break;
                    case "-Infinity":
                        result = Number.NEGATIVE_INFINITY;
                        break;
                    default:
                        throw new Error(`Unexpected number value "${value}"`);
                    }
                } else {
                    result = value;
                }
                break;
            case "bigint":
                result = BigInt(value);
                break;
            case "array": {
                let remoteValue = [];
                result = new RemoteObject(type, remoteValue, objectId);
                for (let child of value) {
                    stack.push({item: child, target: remoteValue});
                }
                break;
            }
            case "set": {
                let remoteValue = new Set();
                result = new RemoteObject(type, remoteValue, objectId);
                for (let child of value) {
                    stack.push({item: child, target: remoteValue});
                }
                break;
            }
            case "object": {
                let remoteValue = {};
                result = new RemoteObject(type, remoteValue, objectId);
                for (let [targetKey, child] of Object.entries(value)) {
                    stack.push({item: child, target: remoteValue, targetKey});
                }
                break;
            }
            case "map": {
                let remoteValue = new Map();
                result = new RemoteObject(type, remoteValue, objectId);
                for (let [targetKey, child] of Object.entries(value)) {
                    stack.push({item: child, target: remoteValue, targetKey});
                }
                break;
            }
            case "sendchannel": {
                result = new SendChannel(value);
                break;
            }
            default:
                result = new RemoteObject(type, value, objectId);
                break;
            }

            if (target === null) {
                if (deserialized !== null) {
                    throw new Error("Tried to create multiple output values");
                }
                deserialized = result;
            } else if (Array.isArray(target)) {
                target.push(result);
            } else {
                target[0][target[1]] = value;
            }
        }
        console.log("Deserialize", obj, deserialized);
        return deserialized;
    }

})();
