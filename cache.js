const redis = require('redis')
const { promisify } = require('util')
const jwtDecoder = require('jwt-decode')
const createError = require('@teamfabric/error')
const { chunk } = require('lodash')
let redisClient


const default_keys = { ...keys }

const getCacheValue = ({ key }) => {
  return connect().then(client => {
    return get(key, client)
  })
}
const setCacheValue = ({ key, ttl, value }) => {
  return connect().then(client => {
    return set(key, ttl, value, client)
  })
}

const setHashValue = (key, values) => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client.hmset(key, values, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}
const getAllHashValue = key => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client.hgetall(key, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}

const getHashValue = (key, field) => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client.hget(key, field, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}

const setZSetValue = (key, values) => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client.zadd(key, values, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}
const getZSetRange = (key, from, to, withScores) => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      if (withScores) {
        client.zrevrange(key, from, to, 'WITHSCORES', (err, res) => {
          if (err) reject(err)
          else resolve(res)
        })
      } else {
        client
          .multi()
          .zrevrange(key, from, to)
          .zcard(key)
          .exec((err, res) => {
            if (err) reject(err)
            else resolve(res)
          })
      }
    })
  })
}
const getZSetRangeByScore = (key, from, to, offset, count) => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client
        .multi()
        .zrevrangebyscore(key, to, from, 'LIMIT', offset, count)
        .zcount(key, from, to)
        .exec((err, res) => {
          if (err) reject(err)
          else resolve(res)
        })
    })
  })
}
const getZSetCount = key => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client.zcard(key, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}
const incZSetScore = (key, fields, inc) => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      const temp = client.multi()
      for (const el of fields) {
        temp.zincrby(key, inc, el)
      }
      temp.exec((err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}
const removeZSetValue = (key, field) => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client.zrem(key, field, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}
const setMKeyValue = values => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client.mset(values, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}
const getMKeyValue = values => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client.mget(values, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}

// Fetch keys matching pattern from Redis. Use this function carefully -- if
// it's ever likely to return more than several thousand keys you should use
// scanKeys() instead.
const getKeys = async ({ pattern }) => {                                                                                    const client = await connect()
  const allKeys = []
  let cursor = 0

  do {
    const [newCursor, keys] = await scanKeys({ pattern, client, cursor })
    allKeys.push(keys)
    cursor = newCursor
  } while (cursor > 0)

  return ['0', allKeys.flat()] // return this format for backwards-compat
}

const deleteKeys = async ({ keys }) => {
  if (!(keys && Array.isArray(keys))) {
    const message = 'keys should be an array'
    throw message
  }
  console.log('Deleting keys from Redis', keys)
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client.del(...keys, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}

const connect = async () => {
  return redisClient || connectRedis()
}

const connectRedis = () => {
  console.log('connectRedis', process.env.REDIS_SERVER_URL)
  return new Promise((resolve, reject) => {
    const client = redis.createClient({
      url: process.env.REDIS_SERVER_URL
    })
    client.on('ready', () => {
      redisClient = client
      resolve(client)
    })
    client.on('error', err => {
      console.error(' Redis error', err)
      redisClient = null
      reject(err)
    })
    client.on('end', () => {
      redisClient = null
    })
  })
}

const get = async (key, client) => {
  const result = await promisify(client.get.bind(client))(key)
  return JSON.parse(result)
}
const set = async (key, ttl, value, client) => {
  if (ttl == undefined || ttl == null) {
    const set = promisify(client.set.bind(client))
    return set(key, JSON.stringify(value))
  } else {
    const setex = promisify(client.setex.bind(client))
    return setex(key, ttl, JSON.stringify(value))
  }
}

const lPushValue = (key, arr) => {
  return connect().then((client) => {
    return new Promise((resolve, reject) => {
      client.lpush(key, arr, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}

const lRangeValues = (key, from=0, to=-1) => {
  return connect().then((client) => {
    return new Promise((resolve, reject) => {
      client.lrange(key, from, to, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}

const lLength = (key) => {
  return connect().then((client) => {
    return new Promise((resolve, reject) => {
      client.llen(key, (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}

// Since we are dealing with potentially a huge number of keys, Redis operates
// with cursors. To create a new cursor, pass in 0. To continue on with a cursor
// and get further results, pass in the cursor returned the previous call. When
// scanKeys() returns a cursor of 0 we have received all results. See Redis
// docs on SCAN for more details.
const scanKeys = async ({ pattern, client, cursor = 0 }) => {
  client = client || (await connect())
  const scan = promisify(client.scan.bind(client))
  return scan(cursor, 'MATCH', pattern, 'COUNT', 1000)
}

// Clear all product keys out of Redis. Depending upon how many keys there are,
// this could take a while.
const clearAllProduct = async (account, includeDeleted) => {
  const startTime = new Date()
  console.log('clearAll start')

  let keysToDelete = []

  // Keys to get individual sku keys from
  const skusKeys = [
    keys.zset,
    keys.inactive_zset,
    keys.zset_child,
    keys.zset_modified,
    keys.in_zset_modified
  ]
  
  // Keys to delete directly
  const otherKeys = [
    keys.idVsSku,
    keys.itemIdVsSku,
    keys.zset,
    keys.zset_child,
    keys.inactive_zset,
    keys.zset_modified,
    keys.in_zset_modified
  ]

  // Get individual sku keys and add to keysToDelete
  for (const skusKey of skusKeys) {
    let keyRes = await getZSetRange(skusKey, 0, -1)
    keyRes = getSafe(() => keyRes[0].map(key => key + '_' + account)) || []
    keysToDelete = keysToDelete.concat(keyRes)
    console.log('clearAll', skusKey, `Len: ${keyRes.length}, Time: ${new Date().getTime() - startTime}`)
  }
  // Add other keys to keysToDelete
  keysToDelete = keysToDelete.concat(otherKeys)
  console.log(`clearAll keysToDelete full Len: ${keysToDelete.length}`)

  if (includeDeleted) {
    keysToDelete.push(keys.deleted_list)
  }

  // Delete keys by chunks
  const keysChunk = chunk(keysToDelete, 1000)
  console.log(`total chunks to delete ${keysChunk.length}`)
  let i = 0
  for (const keys of keysChunk) {
    console.log(`deleting chunk: ${i++}`)
    await deleteKeys({ keys })
  }

  return { message: 'Data cleared successfully' }
}

// Clear all keys in Redis. Only use in tests.
const clearAll = async () => {
  return connect().then(client => {
    return new Promise((resolve, reject) => {
      client.flushall('ASYNC', (err, res) => {
        if (err) reject(err)
        else resolve(res)
      })
    })
  })
}

const getSafe = (fn, defaultVal) => {
  try {
    return fn()
  } catch (e) {
    return defaultVal
  }
}
const str2bool = v => {
  if (typeof v === 'string' && v.toLowerCase() === 'true') {
    return true
  } else if (typeof v === 'boolean' && v) {
    i.state = true
  } else return false
}
const changeKeys = ({ Authorization, context, account }) => {
  if (!account && Authorization) {
    try {
      account = jwtDecoder(Authorization).account
    } catch (error) {
      console.error('Authorization', error)
    }
  }
  if (!account && context) {
    try {
      account = JSON.parse(context).account
    } catch (error) {
      console.error('context', error)
    }
  }
  if (!account) {
    throw createError(
      'USER_NOT_AUTHORIZED',
      403,
      'User is not authorized to access this resource with an explicit deny'
    )()
  }
  for (const key in default_keys) {
    keys[key] = default_keys[key] + '_' + account
  }
  return account
}
module.exports = {
  getCacheValue,
  setCacheValue,
  getKeys,
  deleteKeys,
  scanKeys,
  setZSetValue,
  setMKeyValue,
  getZSetRange,
  getMKeyValue,
  getZSetCount,
  setHashValue,
  getAllHashValue,
  getHashValue,
  removeZSetValue,
  incZSetScore,
  lPushValue,
  lRangeValues,
  lLength,
  getSafe,
  clearAll,
  clearAllProduct,
  keys,
  default_keys,
  changeKeys,
  getZSetRangeByScore,
  str2bool
}
