#!node_modules/.bin/ts-node

// tslint:disable no-console

import request = require('request-promise')
import * as escape from 'pg-escape'
import { Client, Query } from 'pg'
import * as winston from 'winston'
import * as progress from 'cli-progress'

const config = require('./config.json')

const zotero_batch_fetch = 50
const zotero_batch_delete = 1000

const logger = winston.createLogger({
  level: config.logging || 'info',
  format: winston.format.simple(),
  defaultMeta: { service: 'zot2db' },
  transports: [
    //
    // - Write to all logs with level `info` and below to `combined.log`
    // - Write all logs error (and below) to `error.log`.
    //
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' }),
  ],
})

function literal(v) {
  if (Array.isArray(v)) return `{${v.map(literal).join(', ')}}`
  if (v === null) return 'NULL'
  if (typeof v === 'number') return v
  if (typeof v === 'string') return escape.literal(v)
  throw new Error(typeof v)
}

if (config.logging === 'debug') {
  const submit = Query.prototype.submit
  Query.prototype.submit = function() {
    const text = this.text
    const values = this.values || []
    const query = text.replace(/\$([0-9]+)/g, (m, v) => literal(values[parseInt(v) - 1])) + ';'
    // logger.log('debug', query)
    logger.log('debug', text)
    submit.apply(this, arguments)
  }
}

function main(asyncMain) {
  asyncMain()
    .then(exitCode => {
      process.exit(exitCode || 0)
    })
    .catch(err => {
      logger.error(err)
      console.log(err)
      process.exit(1)
    })
}

class Zotero {
  public lmv: number
  public userID: number
  public api_key: string
  public groups: { [key: string]: string } = {} // id => name

  public async login() {
    this.api_key = config[config.api_key] || config.api_key
    const access = await this.get('https://api.zotero.org/keys/current')
    this.userID = access.userID

    for (const group of await this.get(`https://api.zotero.org/users/${this.userID}/groups`)) {
      logger.log('debug', `group: ${group.id} = ${group.data.name} (${group.meta.numItems})`)
      this.groups[group.id] = group.data.name
    }
  }

  public async get(uri = '') {
    const lmv = !uri.startsWith('http')
    if (lmv) uri = `https://api.zotero.org${uri}`
    logger.log('debug', `fetching ${uri}`)

    const res = await request({
      uri,
      headers: {
        'Zotero-API-Version': '3',
        Authorization: `Bearer ${this.api_key}`,
      },
      json: true,
      transform: (body, response) => ({ headers: response.headers, body }),
    })

    if (lmv) {
      if (typeof this.lmv === 'number') {
        if (res.headers['last-modified-version'] !== `${this.lmv}`) {
          throw new Error(`last-modified-version changed from ${this.lmv} to ${res.headers['last-modified-version']} during sync, retry later`)
        }

      } else {
        this.lmv = parseInt(res.headers['last-modified-version'])
        if (isNaN(this.lmv)) throw new Error(`${res.headers['last-modified-version']} is not a number`)

      }
    }

    return res.body
  }
}

const fieldAlias = {
  bookTitle: 'publicationTitle',
  thesisType: 'type',
  university: 'publisher',
  letterType: 'type',
  manuscriptType: 'type',
  interviewMedium: 'medium',
  distributor: 'publisher',
  videoRecordingFormat: 'medium',
  genre: 'type',
  artworkMedium: 'medium',
  websiteType: 'type',
  websiteTitle: 'publicationTitle',
  institution: 'publisher',
  reportType: 'type',
  reportNumber: 'number',
  billNumber: 'number',
  codeVolume: 'volume',
  codePages: 'pages',
  dateDecided: 'date',
  reporterVolume: 'volume',
  firstPage: 'pages',
  caseName: 'title',
  docketNumber: 'number',
  documentNumber: 'number',
  patentNumber: 'number',
  issueDate: 'date',
  dateEnacted: 'date',
  publicLawNumber: 'number',
  nameOfAct: 'title',
  subject: 'title',
  mapType: 'type',
  blogTitle: 'publicationTitle',
  postType: 'type',
  forumTitle: 'publicationTitle',
  audioRecordingFormat: 'medium',
  label: 'publisher',
  presentationType: 'type',
  studio: 'publisher',
  network: 'publisher',
  episodeNumber: 'number',
  programTitle: 'publicationTitle',
  audioFileType: 'medium',
  company: 'publisher',
  proceedingsTitle: 'publicationTitle',
  encyclopediaTitle: 'publicationTitle',
  dictionaryTitle: 'publicationTitle',
}

function bar_format(section, n) {
  return `${section.padStart(n)} [{bar}] {percentage}% | duration: {duration_formatted} ETA: {eta_formatted} | {value}/{total}`
}

function make_insert(table, columns) {
  const variables = [...Array(columns.length + 1).keys()].map(n => `$${n + 1}`)
  const upsert = [...Array(columns.length).keys()].map(n => `${columns[n]} = $${n + 2}`)
  return `
    INSERT INTO sync.${table} ("key", ${columns.join(', ')})
    VALUES (${variables.join(', ')})
    ON CONFLICT("key") DO UPDATE SET
      ${upsert.join(',\n')}
  `
}

main(async () => {
  const dbinfo = typeof config.db === 'string' ? config[config.db] || config.db : config.db
  console.log(`connecting to ${dbinfo.host}...`)
  const db = new Client(dbinfo)
  await db.connect()
  console.log('connected')

  try {
    await db.query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1 AND datname = $2 AND pid <> pg_backend_pid()', [dbinfo.user, dbinfo.database])
  } catch (err) {
    console.log(err)
  }

  const zotero = new Zotero

  await zotero.login()

  const lmv = {}

  const item_fields = Array.from(new Set((await zotero.get('https://api.zotero.org/itemFields')).map(field => fieldAlias[field.field] || field.field)))
  const item_columns = item_fields.map(field => `"${field}"`)

  if (config.reset) {
    logger.warn('reset schema')
    await db.query('DROP SCHEMA IF EXISTS sync CASCADE')
    await db.query('CREATE SCHEMA sync')
  }

  await db.query('BEGIN')

  await db.query('CREATE TABLE IF NOT EXISTS sync.lmv(id VARCHAR(20) PRIMARY KEY, version INT NOT NULL)')
  for (const version of (await db.query('select id, version from sync.lmv')).rows) {
    lmv[version.id] = version.version
  }

  /*
  await db.query('DROP TYPE IF EXISTS sync.item_row')
  await db.query(`
    CREATE TYPE sync.item_row AS (
      "key" VARCHAR(8) NOT NULL,
      "group" VARCHAR,
      itemType VARCHAR(20),
      creators JSONB,
      collections JSONB,
      tags VARCHAR[],
      automatic_tags VARCHAR[],
      ${item_columns.map(col => `${col} VARCHAR`).join(',\n')}
    id      integer,
    field   varchar
    );
  */
  await db.query(`
    CREATE TABLE IF NOT EXISTS sync.items (
      "key" VARCHAR(8) PRIMARY KEY,
      "group" VARCHAR NOT NULL,
      itemType VARCHAR(20) NOT NULL CHECK (itemType NOT IN ('attachment', 'note')),
      creators VARCHAR[],
      collections VARCHAR[],
      tags VARCHAR[],
      automatic_tags VARCHAR[],
      ${item_columns.map(col => `${col} VARCHAR`).join(',\n')}
    )
  `)
  await db.query('CREATE INDEX IF NOT EXISTS items_index_key ON sync.items("key")')

  const attachment_fields = [ 'linkMode', 'title', 'accessDate', 'url', 'note', 'contentType', 'filename', 'dateAdded', 'dateModified' ]
  await db.query(`
    CREATE TABLE IF NOT EXISTS sync.attachments (
      "key" VARCHAR(8) PRIMARY KEY,
      itemType VARCHAR(20) NOT NULL CHECK (itemType IN ('attachment', 'note')),
      parentItem VARCHAR(8) REFERENCES sync.items("key") DEFERRABLE INITIALLY DEFERRED,
      "group" VARCHAR,
      ${attachment_fields.map(col => `${col} VARCHAR`).join(',\n')}
    )
  `)
  await db.query('CREATE INDEX IF NOT EXISTS attachments_index_key ON sync.attachments("key")')
  await db.query('CREATE INDEX IF NOT EXISTS attachments_index_item_key ON sync.attachments(parentItem)')

  const insert_item = make_insert('items', ['itemType', '"group"', 'creators', 'collections', 'tags', 'automatic_tags'].concat(item_columns))
  const insert_attachment = make_insert('attachments', ['itemType', 'parentItem', '"group"'].concat(attachment_fields))

  const groups = [{
    prefix: `/users/${zotero.userID}`,
    name: `u:${zotero.userID}`,
  }]
  for (const [id, name] of Object.entries(zotero.groups)) {
    groups.push({
      prefix: `/groups/${id}`,
      name: `g:${id}:${name}`,
    })
  }
  logger.log('debug', `syncing ${groups.map(g => g.name).join(', ')}`)
  for (const group of groups) {
    lmv[group.prefix] = lmv[group.prefix] || 0
    zotero.lmv = null // because this is per-group, first request must get the LMV

    logger.info('group:', group.name)
    const deleted = await zotero.get(`${group.prefix}/deleted?since=${lmv[group.prefix]}`)
    if (zotero.lmv === lmv[group.prefix]) {
      logger.info(`${group.name}: up to date`)
      continue
    }

    console.log(group.name)
    let bar = new progress.Bar({ format: bar_format('purge', 12) })
    bar.start(deleted.items.length, 0)
    while (deleted.items.length) {
      logger.log('debug', `delete: ${deleted.items.length}`)
      const batch = deleted.items.splice(0, zotero_batch_delete)
      // https://github.com/brianc/node-postgres/issues/1653
      await db.query('DELETE FROM sync.items WHERE "key" = ANY($1)', [batch])
      await db.query('DELETE FROM sync.attachments WHERE "key" = ANY($1)', [batch])
      bar.update(bar.total - deleted.items.length)
    }
    bar.update(bar.total)
    bar.stop()

    const collections = {}
    const collection_versions = Object.keys(await zotero.get(`${group.prefix}/collections?since=0&format=versions`))
    bar = new progress.Bar({ format: bar_format('collections', 12) })
    bar.start(collection_versions.length, 0)
    while (collection_versions.length) {
      const batch = (await zotero.get(`${group.prefix}/collections?collectionKey=${collection_versions.splice(0, zotero_batch_fetch).join(',')}`)).map(coll => coll.data)
      for (const coll of batch) {
        collections[coll.key] = {
          name: coll.name,
          parent: coll.parentCollection || null,
        }
      }
      bar.update(bar.total - collection_versions.length)
    }
    bar.update(bar.total)
    bar.stop()

    const items = Object.keys(await zotero.get(`${group.prefix}/items?since=${lmv[group.prefix]}&format=versions`))
    if (config.limit) items.splice(config.limit, items.length)
    bar = new progress.Bar({ format: bar_format('items', 12) })
    bar.start(items.length, 0)
    while (items.length) {
      logger.log('debug', `fetch: ${items.length}`)

      const batch = (await zotero.get(`${group.prefix}/items?itemKey=${items.splice(0, zotero_batch_fetch).join(',')}`)).map(item => item.data)

      for (const item of batch) {
        switch (item.itemType) {
          case 'attachment':
          case 'note':
            await db.query(insert_attachment, [
              item.key, 
              item.itemType,
              item.parentItem || null,
              group.name,
            ].concat(attachment_fields.map(col => item[col] || null)))
            break

          case 'note':
            console.log(item)
            await db.query('INSERT INTO sync.notes ("key", itemKey, "group", note) VALUES ($1, $2, $3, $4)', [item.key, item.parentItem || null, group.name, item.note || ''])
            break

          default:
            for (const [alias, field] of Object.entries(fieldAlias)) {
              if (item[alias]) item[field] = item[alias]
            }

            const row = [
              item.key,
              item.itemType,
              group.name,
              (item.creators || []).map(c => [c.name, c.lastName, c.firstName].filter(n => n).join(', ')),
              (item.collections || []).map(key => collections[key] ? collections[key].name : null).filter(coll => coll),
              (item.tags || []).filter(tag => tag.type !== 1).map(tag => tag.tag),
              (item.tags || []).filter(tag => tag.type === 1).map(tag => tag.tag),
            ].concat(item_fields.map(col => item[col] || null))
            await db.query(insert_item, row)
            break
        }
      }

      bar.update(bar.total - items.length)
    }
    bar.update(bar.total)
    bar.stop()

    await db.query('INSERT INTO sync.lmv (id, version) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET version = $2', [group.prefix, zotero.lmv])
  }

  logger.log('debug', 'commit')
  await db.query('COMMIT')

  await db.query('DROP MATERIALIZED VIEW IF EXISTS public.items')
  const view = `
    CREATE MATERIALIZED VIEW public.items AS
    WITH notes AS (
      SELECT parentItem, string_agg(extra, '\\n' ORDER BY "key") as notes
      FROM sync.items
      WHERE parentItem IS NOT NULL
      GROUP BY parentItem
    )
    SELECT
      "group",
      array_to_string(creators, ' ') as creators,
      collection,
      tag,
      automatic_tag,
      notes.notes,
      ${item_columns.join(', ')}
    FROM sync.items
    LEFT JOIN LATERAL unnest(collections) AS _collections(collection) ON TRUE
    LEFT JOIN LATERAL unnest(tags) AS _tags(tag) ON TRUE
    LEFT JOIN LATERAL unnest(automatic_tags) AS _automatic_tags(automatic_tag) ON TRUE
    LEFT JOIN notes ON notes.parentItem = "key"
    WHERE sync.items.parentItem IS NULL
  `
  await db.query(view)
  await db.query('REFRESH MATERIALIZED VIEW public.items')

  const index = config.index || []
  for (const col of index) {
    if (!['collection', 'group', 'tag', 'automatic_tag'].includes(col) && !item_fields.includes(col)) continue
    await db.query(`CREATE INDEX IF NOT EXISTS items_index_${col} ON public.items("${col}")`)
  }

  if (config.clear) {
    await db.query('DROP SCHEMA IF EXISTS sync CASCADE')
    await db.query('CREATE SCHEMA sync')
  }
})
