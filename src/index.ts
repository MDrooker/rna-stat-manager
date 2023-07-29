import Redis, { Cluster, ClusterNode, ClusterOptions, RedisOptions } from 'ioredis';
import Debug from 'debug';

const debug = Debug('rna-stat-manager');

type RedisOrCluster = Redis | Cluster;

export type {
    Redis,
    Cluster,
    ClusterNode,
    ClusterOptions,
    RedisOptions,
};

export type ClusterNodeOptions = {
    nodes: ClusterNode[];
    options: ClusterOptions;
};

export type CounterServiceConfig = {
    app: {
        system: string;
        product: string;
        environment: string;
    };
    options: RedisOptions | ClusterOptions;
    nodes?: ClusterNode[];
};

export const createRedis = (config: CounterServiceConfig) => new Redis(config.options);

export const createRedisCluster = (config: CounterServiceConfig) => new Redis.Cluster(config.nodes!, config.options);

export class CounterService {
    private config: CounterServiceConfig;
    private redis: RedisOrCluster;
    private isCluster: boolean;

    constructor(config: CounterServiceConfig) {
        this.config = { ...config };
        this.isCluster = Array.isArray(this.config.nodes) && this.config.nodes.length > 0;
        this.redis = this.isCluster ? createRedisCluster(this.config) : createRedis(this.config);
    }

    async count(name: string): Promise<string | null> {
        const key = this.namedCounterKey(name);
        return this.client.get(key);
    }

    async counterKeys(name: string): Promise<{count: number, results: string[]}> {
        const key = this.namedCounterKey(name);
        
        if (!key.endsWith('*')) {
            return {
                count: 1,
                results: [key],
            };
        }
        
        if (this.isCluster) {
            throw new Error('Cluster mode does not support scanStream');
        }

        let count = 0;
        const results: string[] = [];
        return new Promise((resolve, reject) => {
            try {
                const client = this.client as Redis
                let stream = client.scanStream({
                    match: key,
                    count: 100,
                });
                stream.on('data', (keys: string[]) => {
                    if (keys.length > 0) {
                        count += keys.length
                        results.push(...keys)
                    }
                });
                stream.on('end', () => {
                    resolve({ count, results });
                });
            } catch (err: unknown) {
                console.error(err);
                reject(err);
            }
        });
    }

    async incr(name: string, amount = 1, expirySeconds?: number): Promise<number> {
        const key = this.namedCounterKey(name);
        debug(`incr ${key} by ${amount}`);
        const value = await this.client.incrby(key, amount);
        
        if (expirySeconds) {
            await this.client.expire(key, expirySeconds);
        }
        
        return value;
    }

    async decr(name: string, amount = 1, expirySeconds?: number): Promise<number> {
        const key = this.namedCounterKey(name);
        debug(`decr ${key} by ${amount}`);
        const value = await this.client.decrby(key, amount);
        
        if (expirySeconds) {
            await this.client.expire(key, expirySeconds);
        }
        
        return value;
    }

    async set(name: string, value: number, expirySeconds?: number): Promise<"OK"> {
        const key = this.namedCounterKey(name);
        debug(`setting ${key} to ${value}`);

        if (expirySeconds) {
            return this.client.setex(key, expirySeconds, value);
        }
        
        return this.client.set(key, value);
    }

    async incrHashField(name: string, field: string, amount = 1, expirySeconds?: number): Promise<number> {
        const key = this.namedCounterKey(name);
        debug(`incr hash ${key} field ${field} by ${amount}`);
        const value = await this.client.hincrby(key, field, amount);

        if (expirySeconds) {
            await this.client.expire(key, expirySeconds);
        }

        return value;
    }

    async decrHashField(name: string, field: string, amount = 1, expirySeconds?: number): Promise<number> {
        const key = this.namedCounterKey(name);
        debug(`decr hash ${key} field ${field} by ${amount}`);
        const value = await this.client.hincrby(key, field, amount);

        if (expirySeconds) {
            await this.client.expire(key, expirySeconds);
        }

        return value;
    }

    async hashCounts(name: string): Promise<Record<string, string>> {
        let key = this.namedCounterKey(name);
        return this.client.hgetall(key);
    }

    async delete(name: string): Promise<number> {
        const key = this.namedCounterKey(name);
        debug(`delete key ${key}`);
        return this.client.del(key);
    }

    private get client(): RedisOrCluster {
        return this.redis;
    }

    private counterKeyPrefix(): string {
        const { system, product, environment } = this.config.app;
        return `app:${system}:${product}:${environment}`
    }

    private namedCounterKey(name: string): string {
        return `${this.counterKeyPrefix()}:cnt:${name}}`;
    }
}
