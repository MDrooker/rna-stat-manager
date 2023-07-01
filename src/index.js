import os from "os";
import { debugModule } from '@mdrooker/rna-logger';
import Redis from "ioredis";

const systemHost = os.hostname().toLowerCase();
const debug = new debugModule("rna:stats-manager");
const appDebug = new debugModule("app:stats-manager");

let _redisConfig;
let _instanceurn;
let _publisher
let _config;
let _options;
class StatsService {
    init({ config, instanceurn }) {
        if (config && config.REDIS) {
            this._config = config;
            this._redisConfig = config.REDIS;
            this._options = {
                connectionName: 'redis',
                host: this._redisConfig.URL,
                port: this._redisConfig.PORT,
                password: this._redisConfig.PASSWORD ? this._redisConfig.PASSWORD : null,
                connectTimeout: this._redisConfig.TIMEOUT ? this._redisConfig.TIMEOUT : 20000,
                compress: true,
                maxRetriesPerRequest: 10
            };
        } else {
            throw new Error('No config passed to StatsService init method');
        }
        if (instanceurn) {
            this._instanceurn = instanceurn
        }
        _publisher = new Redis(this._options);
        return _publisher

    }
    get publisher() {
        if (_publisher) {
            return _publisher
        }
        else {
            _publisher = getPublisher();
            return _publisher
        }
    }

    statKeyPrefix() {
        return `app:${this._config.NAME.SYSTEM}:${this._config.NAME.PRODUCT}:${this._config.ENVIRONMENT}`
    }
    namedStatKey({ global, instanceurn, type, name, host = systemHost }) {
        if (global) {
            if (type) {
                return `${this.statKeyPrefix()}:stat:${type}:${name}`
            } else {
                return `${this.statKeyPrefix()}:stat:${name}`
            }
        } else {
            if (instanceurn) {
                if (host) {
                    return `${this.statKeyPrefix()}:stat:${type}:${name}:${host}:${instanceurn}`

                } else {
                    return `${this.statKeyPrefix()}:stat:${type}:${name}:${instanceurn}`
                }
            } else {
                if (host) {
                    return `${this.statKeyPrefix()}:stat:${type}:${name}:${host}`

                } else {
                    return `${this.statKeyPrefix()}:stat:${type}:${name}`
                }
            }
        }
    }
    namedHashKey({ global, name, instanceurn, host = systemHost }) {
        if (global) {
            return `${this.statKeyPrefix()}:${name}`
        }
    }
    async getStatValue({ global, instanceurn, type, name }) {
        let key = this.namedStatKey({ global, type, name: `${name}`, instanceurn })
        if (key.includes('*')) {
            let results = [];
            let count = 0;
            const startedTime = new Date();
            return new Promise((resolve, reject) => {
                try {
                    let stream = this.publisher.scanStream({
                        match: `${key}`,
                        // returns approximately 100 elements per call
                        count: 1000,
                    });
                    stream.on('data', function (keys) {
                        if (keys.length > 0) {
                            debugger
                            // `keys` is an array of strings representing key name
                            count += keys.length
                            results.push(keys)
                        }
                    });
                    stream.on('end', function () {
                        let elapsed = new Date() - startedTime
                        resolve({
                            count: count,
                            results: results,
                        });
                    });
                } catch (error) {
                    console.log(error)
                }
            })

        } else {
            debug(`Geting Stat Value for ${key}`)
            return await this.publisher.get(key);
        }
    }
    async decrStatValue({ global, host, instanceurn, type, name, value = 1, expiresAtInSeconds, fast = false }) {
        let newValue;
        let key = this.namedStatKey({ global, host, type: `${type}`, name: `${name}`, instanceurn })
        debug(`Decrementing Value ${value} for key  ${key}`)
        if (fast) {
            newValue = this.publisher.decrby(key, value);
        } else {
            newValue = await this.publisher.decrby(key, value);
        }
        if (expiresAtInSeconds) {
            this.publisher.expire(key, expiresAtInSeconds);
        }
        return newValue
    }
    async incrStatValue({ global, host, instanceurn, type, name, value = 1, expiresAtInSeconds, fast = false }) {
        let newValue;
        let key = this.namedStatKey({ global, host, type: `${type}`, name: `${name}`, instanceurn, host });
        debug(`Incrementing Value ${value} for key  ${key}`)
        if (fast) {
            newValue = this.publisher.incrby(key, value);
        } else {
            newValue = await this.publisher.incrby(key, value);
        }
        if (expiresAtInSeconds) {
            this.publisher.expire(key, expiresAtInSeconds);
        }
        return newValue
    }

    setStatValue({ global, instanceurn, type, name, value = 1, expiresAtInSeconds }) {
        let key = this.namedStatKey({ global, type: `${type}`, name: `${name}`, instanceurn })
        if (typeof (value) === 'object') {
            debug(`Setting Value ${JSON.stringify(value)} for key ${key}`)
        } else {
            debug(`Setting Value ${value} for key  ${key}`)
        }
        if (expiresAtInSeconds) {
            this.publisher.setex(key, expiresAtInSeconds, JSON.stringify({ value: value, setTime: new Date() }));
        }
        else {
            this.publisher.set(key, JSON.stringify({ value: value, setTime: new Date() }));
        }
    }
    async incrHashValue({ global, host, instanceurn, name, field, value = 1, expiresAtInSeconds, fast = false }) {
        let newValue;
        let key = this.namedHashKey({ global, host, name: `${name}`, instanceurn, host });
        debug(`Incrementing Hash Value ${name} for key ${key}`)
        const pipeline = this.publisher.pipeline();
        if (fast) {
            newValue = pipeline.hincrby(key, field, value);
        } else {
            newValue = await pipeline.hincrby(key, field, value);
        }
        if (expiresAtInSeconds) {
            pipeline.expire(key, expiresAtInSeconds);
        }
        pipeline.exec((err, result) => {
            return newValue
        });
    }
    async decrHashValue({ global, host, instanceurn, name, field, value = -1, expiresAtInSeconds, fast = false }) {
        let newValue;
        let key = this.namedHashKey({ global, host, name: `${name}`, instanceurn, host });
        debug(`Incrementing Hash  Value ${name} for key ${key}`)
        const pipeline = this.publisher.pipeline();
        if (fast) {
            newValue = pipeline.hincrby(key, type, value);
        } else {
            newValue = await pipeline.hincrby(key, type, value);
        }
        if (expiresAtInSeconds) {
            pipeline.expire(key, expiresAtInSeconds);
        }
        pipeline.exec((err, result) => {
            return newValue
        });

    }
    async getHashValues({ global, host, instanceurn, name }) {
        let key = this.namedHashKey({ global, host, name: `${name}`, instanceurn, host });
        debug(`Getting Hash Values for key ${key}`);
        return await this.publisher.hgetall(key);
    }


    removeStatValue({ instanceurn, type, name }) {
        let key = this.namedStatKey({ type: `${type}`, name: `${name}`, instanceurn })
        debug(`Deleting Value ${value} for key  ${key}`)
        this.publisher.del(key);
    }
    removeAllKeysForInstance({ instanceurn }) {
        let buildKey = this.namedStatKey({ instanceurn: instanceurn, type: "*", name: "*" });
        debug(`Removing All Keys for ${buildKey}`)
        try {
            let stream = this.publisher.scanStream({
                match: `${buildKey}`
            });
            stream.on('data', function (keys) {
                // `keys` is an array of strings representing key names
                if (keys.length) {
                    keys.forEach(function (key) {
                        this.publisher.del(key);
                    });
                }
            });
            stream.on('end', function () {
                debug('scanStream done');
            });
        }
        catch (error) {
            console.log(error);
        }
    }
    async getSubscriptionCounts({ instanceurn }) {
        let subscriptionKey = this.namedStatKey({ instanceurn: instanceurn, type: "count", name: "subscription", host: null });
        try {
            debug(`Search for Keys ${subscriptionKey}`);
            let keys = await this.publisher.keys(subscriptionKey);
            if (keys.length > 0) {
                let get = await this.publisher.mget(keys);
                return {
                    keys: keys,
                    data: get,
                    hello: 1
                };
            }
            else {
            }

        } catch (error) {
            console.log(error)
            throw new Error(error)
        }
    }
    async getOnlineServerCount() {
        let count = 0
        const startedTime = new Date();

        let buildKey = this.namedStatKey({ host: "*", instanceid: "*", type: "count", name: "online" })
        debug(`Getting Online Server Count for ${buildKey}`)
        return new Promise((resolve, reject) => {
            try {
                let stream = this.publisher.scanStream({
                    match: `${buildKey}`,
                    // returns approximately 100 elements per call
                    count: 100,
                });
                stream.on('data', function (keys) {
                    // `keys` is an array of strings representing key name
                    count += keys.length
                });
                stream.on('end', function () {
                    let elapsed = new Date() - startedTime
                    resolve(count);
                });
            } catch (error) {
                console.log(error)
            }
        })
    }
}
export default new StatsService();
