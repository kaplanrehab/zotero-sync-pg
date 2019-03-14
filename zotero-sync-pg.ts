#!node_modules/.bin/ts-node

// tslint:disable no-console

import request = require('request-promise')
import { prepareValue } from 'pg/lib/utils'
import { Client, Query } from 'pg'
import * as winston from 'winston'
import * as progress from 'cli-progress'
import * as fs from 'fs'
import * as ini from 'ini'
import * as path from 'path'
import AJV = require('ajv')

const config = ini.parse(fs.readFileSync(path.join(__dirname, 'config.ini'), 'utf-8'))
const validator = (new AJV({ useDefaults: true, coerceTypes: true })).compile(require('./config.json'))
if (!validator(config)) {
  console.log(validator.errors)
  process.exit(1)
}

const zotero_batch_fetch = 50
const zotero_batch_delete = 1000
const prompt_width = 16

const logger = winston.createLogger({
  level: config.sync.logging,
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

if (config.sync.logging === 'debug') {
  const submit = Query.prototype.submit
  Query.prototype.submit = function() {
    try {
      const text = this.text
      const values = this.values || []
      const query = `SQL: ${text.replace(/\$([0-9]+)/g, (m, v) => prepareValue(values[parseInt(v) - 1])).trim()};`
      logger.log('debug', query)
    } catch (err) {
      logger.error(err)
    }
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
    this.api_key = config[config.zotero.api_key] || config.zotero.api_key
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
  return `${section.padEnd(n)} [{bar}] {percentage}% | duration: {duration_formatted} ETA: {eta_formatted} | {value}/{total}`
}

function field2column(field) {
  return field.replace(/[a-z][A-Z]/g, pair => `${pair[0]}_${pair[1]}`).toLowerCase()
}
function field2quoted_column(field) {
  return `"${field2column(field)}"`
}

function make_insert(entity, fields) {
  const columns = fields.map(field2quoted_column)
  const upsert = columns.map(col => `${col} = EXCLUDED.${col}`)

  return `
    INSERT INTO sync.${entity}s ("key", ${columns.join(', ')})
    SELECT rs."key", ${columns.map(col => `rs.${col}`).join(', ')} FROM json_populate_recordset(null::sync.${entity}_row, $1) AS rs
    ON CONFLICT("key") DO UPDATE SET
      ${upsert.join(',\n')}
  `
}
/*
function make_select(entity, fields) {
  const columns = fields.map(field2quoted_column)
  return `
    SELECT rs."key", ${columns.map(col => `rs.${col}`).join(', ')} FROM json_populate_recordset(null::sync.${entity}_row, $1) AS rs
  `
}
*/

main(async () => {
  console.log(`connecting to ${config.db.host}...`)
  const db = new Client(config.db)
  await db.connect()
  console.log('connected')

  try {
    await db.query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1 AND datname = $2 AND pid <> pg_backend_pid()', [config.db.user, config.db.database])
  } catch (err) {
    console.log(err)
  }

  const zotero = new Zotero

  await zotero.login()

  const lmv = {}

  const item_fields: string[] = Array.from(new Set((await zotero.get('https://api.zotero.org/itemFields')).map(field => fieldAlias[field.field] || field.field)))
  const item_columns = item_fields.map(field2quoted_column)

  if (config.db.reset) {
    logger.warn('reset schema')
    await db.query('DROP SCHEMA IF EXISTS sync CASCADE')
    await db.query('CREATE SCHEMA sync')
  }

  await db.query('CREATE TABLE IF NOT EXISTS sync.lmv(id VARCHAR(20) PRIMARY KEY, version INT NOT NULL)')
  for (const version of (await db.query('select id, version from sync.lmv')).rows) {
    lmv[version.id] = version.version
  }

  await db.query(`
    CREATE TABLE IF NOT EXISTS sync.items (
      "key" VARCHAR(8) PRIMARY KEY,
      "parent_item" VARCHAR(8) REFERENCES sync.items("key") DEFERRABLE INITIALLY DEFERRED CHECK ("parent_item" IS NULL OR "item_type" IN ('attachment', 'note')),
      "group" VARCHAR NOT NULL,
      "item_type" VARCHAR(20),
      "creators" VARCHAR[],
      "collections" VARCHAR[],
      "tags" VARCHAR[],
      "automatic_tags" VARCHAR[],
      ${item_columns.map(col => `${col} VARCHAR`).join(',\n')}
    )
  `)
  await db.query('CREATE INDEX IF NOT EXISTS items_index_key ON sync.items("key")')
  await db.query('CREATE INDEX IF NOT EXISTS items_parent_key ON sync.items("parent_item")')
  await db.query('DROP TYPE IF EXISTS sync.item_row')
  await db.query(`
    CREATE TYPE sync.item_row AS (
      "key" VARCHAR(8),
      "parent_item" VARCHAR(8),
      "group" VARCHAR,
      "item_type" VARCHAR(20),
      "creators" VARCHAR[],
      "collections" VARCHAR[],
      "tags" VARCHAR[],
      "automatic_tags" VARCHAR[],
      ${item_columns.map(col => `${col} VARCHAR`).join(',\n')}
    )
  `)

  // const show_items = make_select('item', ['parentItem', 'itemType', 'group', 'creators', 'collections', 'tags', 'automatic_tags'].concat(item_fields))
  const insert_items = make_insert('item', ['parentItem', 'itemType', 'group', 'creators', 'collections', 'tags', 'automatic_tags'].concat(item_fields))

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
    await db.query('BEGIN')

    lmv[group.prefix] = lmv[group.prefix] || 0
    zotero.lmv = null // because this is per-group, first request must get the LMV

    logger.info('group:', group.name)
    const deleted = await zotero.get(`${group.prefix}/deleted?since=${lmv[group.prefix]}`)
    if (zotero.lmv === lmv[group.prefix]) {
      logger.info(`${group.name}: up to date`)
      continue
    }

    console.log(group.name)

    const collections = {}
    const collection_versions = Object.keys(await zotero.get(`${group.prefix}/collections?since=0&format=versions`))
    let bar = new progress.Bar({ format: bar_format('collections', prompt_width) })
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

    bar = new progress.Bar({ format: bar_format('items:delete', prompt_width) })
    bar.start(deleted.items.length, 0)
    while (deleted.items.length) {
      logger.log('debug', `delete: ${deleted.items.length}`)
      const batch = deleted.items.splice(0, zotero_batch_delete)
      // https://github.com/brianc/node-postgres/issues/1653
      await db.query('DELETE FROM sync.items WHERE "key" = ANY($1)', [batch])
      bar.update(bar.total - deleted.items.length)
    }
    bar.update(bar.total)
    bar.stop()

    const items = Object.keys(await zotero.get(`${group.prefix}/items?since=${lmv[group.prefix]}&format=versions`))
    if (config.zotero.limit) items.splice(config.zotero.limit, items.length)
    bar = new progress.Bar({ format: bar_format('items:add/update', prompt_width) })
    bar.start(items.length, 0)
    while (items.length) {
      logger.log('debug', `fetch: ${items.length}`)

      const batch = {
        fetched: (await zotero.get(`${group.prefix}/items?itemKey=${items.splice(0, zotero_batch_fetch).join(',')}`)).map(item => item.data),
        items: [],
      }

      logger.log('debug', `batch = ${JSON.stringify(batch.fetched.map(item => ({key: item.key, type: item.itemType, parent: item.parentItem })))}`)

      for (const item of batch.fetched) {
        const row: { [key: string]: string } = {
          key: item.key,
          group: group.name,
          item_type: item.itemType,
        }

        switch (item.itemType) {
          case 'attachment':
          case 'note':
            for (const field of item_fields) {
              row[field2column(field)] = item[field] || null
            }

            row.parent_item = item.parentItem || null
            // re-use columns
            row.place = item.filename || null
            row.archive_location = item.linkMode || null
            row.abstract_note = item.note || null
            row.type = item.contentType || null

            batch.items.push(row)
            break

          default:
            for (const [alias, field] of Object.entries(fieldAlias)) {
              if (item[alias]) item[field] = item[alias]
            }
            for (const field of item_fields) {
              row[field2column(field)] = item[field] || null
            }
            row.creators = (item.creators || []).map(c => [c.name, c.lastName, c.firstName].filter(n => n).join(', '))
            row.collections = (item.collections || []).map(key => collections[key] ? collections[key].name : null).filter(coll => coll)
            row.tags = (item.tags || []).filter(tag => tag.type !== 1).map(tag => tag.tag)
            row.automatic_tags = (item.tags || []).filter(tag => tag.type === 1).map(tag => tag.tag)

            batch.items.push(row)
            break
        }
      }

      if (batch.items.length) await db.query(insert_items, [JSON.stringify(batch.items)])

      bar.update(bar.total - items.length)
    }
    bar.update(bar.total)
    bar.stop()

    await db.query('INSERT INTO sync.lmv (id, version) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET version = $2', [group.prefix, zotero.lmv])
    await db.query('COMMIT')
  }

  await db.query('DROP MATERIALIZED VIEW IF EXISTS public.items')
  const view = `
    CREATE MATERIALIZED VIEW public.items AS
    WITH notes AS (
      SELECT parent_item, string_agg(abstract_note, '\\n' ORDER BY "key") as notes
      FROM sync.items
      WHERE parent_item IS NOT NULL AND item_type = 'note'
      GROUP BY parent_item
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
    LEFT JOIN notes ON notes.parent_item = "key"
    WHERE item_type NOT IN ('attachment', 'note')
  `
  await db.query(view)
  await db.query('REFRESH MATERIALIZED VIEW public.items')

  const columns = (await db.query(`
    SELECT mv.attname as col
    FROM pg_attribute mv
    JOIN pg_class t on mv.attrelid = t.oid
    JOIN pg_namespace s on t.relnamespace = s.oid
    WHERE mv.attnum > 0
      AND NOT mv.attisdropped
      AND t.relname = 'items'
      AND s.nspname = 'public'
  `)).rows.map(col => col.col)
  let shown = false

  config.db.index = config.db.index.split(',').map(col => col.trim()).filter(col => col)
  for (const always of ['tag', 'automatic_tag', 'collection']) {
    if (!config.db.index.includes(always)) config.db.index.push(always)
  }
  for (const col of config.db.index) {
    if (!columns.includes(col)) {
      console.log(`not indexing unknown column ${col}`)
      if (!shown) console.log(`available for index: ${columns.join(', ')}`)
      shown = true
    } else {
      await db.query(`CREATE INDEX IF NOT EXISTS items_${col} ON public.items("${col}")`)
    }
  }

  if (config.db.clear) {
    await db.query('DROP SCHEMA IF EXISTS sync CASCADE')
    await db.query('CREATE SCHEMA sync')
  }
})
