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

const pkg = require('./package.json')
const program = require('commander')
program
  .option('-r, --reset', 'Reset sync')
  .parse(process.argv)

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
      console.log('main failed:', err.stack)
      logger.error(err)
      process.exit(1)
    })
}

class Zotero {
  public lmv: number
  public userID: number
  public api_key: string
  public groups: { [key: string]: { user_or_group_prefix: string, name: string } } = {} // id => name

  public async login() {
    this.api_key = config[config.zotero.api_key] || config.zotero.api_key
    const account = await this.get('https://api.zotero.org/keys/current')
    this.userID = account.userID

    if (account.access && account.access.user && account.access.user.library) {
      const user_or_group_prefix = `/users/${this.userID}`
      this.groups[user_or_group_prefix] = {
        user_or_group_prefix,
        name: 'My Library',
      }
    }

    for (const group of await this.get(`https://api.zotero.org/users/${this.userID}/groups`)) {
      const user_or_group_prefix = `/groups/${group.id}`
      this.groups[user_or_group_prefix] = {
        user_or_group_prefix,
        name: group.data.name,
      }
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

const save = new class {
  private queries = []
  private bar = new progress.Bar({ format: bar_format('saving', prompt_width) })
  private total = 0

  public add(q, batch) {
    this.total += batch
    this.queries.push(async () => {
      await q
      this.bar.increment(batch)
    })
  }

  public async start() {
    this.bar.start(this.total, 0)
    await Promise.all(this.queries)
    this.bar.update(this.total)
    this.bar.stop()
  }
}

const thousand = 1000
const million = thousand * thousand
function version_to_int(v) {
  const [major, minor, patch] = v.split('.').map(p => parseInt(p))
  return (major * million) + (minor * thousand) + patch
}
function int_to_version(v) {
  const major = Math.floor(v / million)
  v = v - (major * million)
  const minor = Math.floor(v / thousand)
  v = v - (minor * thousand)
  const patch = v
  return `${major}.${minor}.${patch}`
}

main(async () => {
  console.log(`${pkg.name} ${pkg.version}: connecting to ${config.db.host}...`)
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

  const lmv: { [key: string]: number } = {}

  const item_fields: string[] = Array.from(new Set((await zotero.get('https://api.zotero.org/itemFields')).map(field => fieldAlias[field.field] || field.field)))
  const item_columns = item_fields.map(field2quoted_column)

  if (program.reset) {
    logger.warn('reset schema')
    await db.query('DROP SCHEMA IF EXISTS sync CASCADE')
    await db.query('CREATE SCHEMA sync')
  }

  await db.query(`CREATE TABLE IF NOT EXISTS sync.lmv (
      user_or_group_prefix VARCHAR(20) PRIMARY KEY,
      group_name VARCHAR NOT NULL,
      version INT NOT NULL
    )
  `)

  await db.query(`
    CREATE TABLE IF NOT EXISTS sync.items (
      "key" VARCHAR(8) PRIMARY KEY,
      "parent_item" VARCHAR(8) REFERENCES sync.items("key") DEFERRABLE INITIALLY DEFERRED CHECK ("parent_item" IS NULL OR "item_type" IN ('attachment', 'note')),
      user_or_group_prefix VARCHAR(20) NOT NULL REFERENCES sync.lmv(user_or_group_prefix) DEFERRABLE INITIALLY DEFERRED,
      "deleted" BOOLEAN NOT NULL,
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
      user_or_group_prefix VARCHAR(20),
      "deleted" BOOLEAN,
      "item_type" VARCHAR(20),
      "creators" VARCHAR[],
      "collections" VARCHAR[],
      "tags" VARCHAR[],
      "automatic_tags" VARCHAR[],
      ${item_columns.map(col => `${col} VARCHAR`).join(',\n')}
    )
  `)

  await db.query(`
    CREATE TABLE sync.collections (
      "key" VARCHAR(8) NOT NULL,
      user_or_group_prefix VARCHAR(20) NOT NULL REFERENCES sync.lmv(user_or_group_prefix) DEFERRABLE INITIALLY DEFERRED,
      "parent_collection" VARCHAR(8),
      "name" VARCHAR
    )
  `)

  const insert_items = make_insert('item', ['parentItem', 'itemType', 'user_or_group_prefix', 'deleted', 'creators', 'collections', 'tags', 'automatic_tags'].concat(item_fields))

  logger.log('debug', `syncing ${Object.values(zotero.groups).map(g => g.name).join(', ')}`)

  // remove groups we no longer have access to
  await db.query('DELETE FROM sync.items WHERE NOT (user_or_group_prefix = ANY($1))', [ Object.values(zotero.groups).map(g => g.user_or_group_prefix) ])
  await db.query('DELETE FROM sync.collections WHERE NOT (user_or_group_prefix = ANY($1))', [ Object.values(zotero.groups).map(g => g.user_or_group_prefix) ])
  await db.query("DELETE FROM sync.lmv WHERE user_or_group_prefix <> 'sync' AND NOT (user_or_group_prefix = ANY($1))", [ Object.values(zotero.groups).map(g => g.user_or_group_prefix) ])

  for (const group of (await db.query('select user_or_group_prefix, version from sync.lmv')).rows) {
    lmv[group.user_or_group_prefix] = group.version
  }

  if (!lmv.sync) {
    lmv.sync = version_to_int(pkg.version)
  } else if (version_to_int(pkg.version) < lmv.sync) {
    console.log(`database was synced with version ${int_to_version(lmv.sync)}, please upgrade your scripts`)
    process.exit(1)
  }

  for (const group of Object.values(zotero.groups)) {
    await db.query('BEGIN')

    lmv[group.user_or_group_prefix] = lmv[group.user_or_group_prefix] || 0
    zotero.lmv = null // because this is per-group, first request must get the LMV

    logger.info('debug', `group:${group.user_or_group_prefix}::${group.name}`)
    const deleted = await zotero.get(`${group.user_or_group_prefix}/deleted?since=${lmv[group.user_or_group_prefix]}`)
    if (zotero.lmv === lmv[group.user_or_group_prefix]) {
      console.log(`${group.name}: up to date`)
      logger.info(`${group.name}: up to date`)
      continue
    }

    console.log(group.user_or_group_prefix, group.name)

    await db.query('DELETE FROM sync.collections WHERE user_or_group_prefix = $1', [group.user_or_group_prefix])
    const collections = Object.keys(await zotero.get(`${group.user_or_group_prefix}/collections?since=0&format=versions`))
    let bar = new progress.Bar({ format: bar_format('collections', prompt_width) })
    bar.start(collections.length, 0)
    while (collections.length) {
      const batch = (await zotero.get(`${group.user_or_group_prefix}/collections?collectionKey=${collections.splice(0, zotero_batch_fetch).join(',')}`)).map(coll => coll.data)

      await db.query(
        'INSERT INTO sync.collections (user_or_group_prefix, "key", "parent_collection", "name") SELECT * FROM UNNEST ($1::varchar[], $2::varchar[], $3::varchar[], $4::varchar[])', [
          batch.map(coll => group.user_or_group_prefix),
          batch.map(coll => coll.key),
          batch.map(coll => coll.parentCollection || null),
          batch.map(coll => coll.name),
        ]
      )

      bar.update(bar.total - collections.length)
    }
    bar.update(bar.total)
    bar.stop()

    bar = new progress.Bar({ format: bar_format('items:delete', prompt_width) })
    bar.start(deleted.items.length, 0)
    while (deleted.items.length) {
      logger.log('debug', `delete: ${deleted.items.length}`)

      save.add(db.query('DELETE FROM sync.items WHERE "key" = ANY($1)', [ deleted.items.splice(0, zotero_batch_delete) ]), 1)

      bar.update(bar.total - deleted.items.length)
    }
    bar.update(bar.total)
    bar.stop()

    const items = Object.keys(await zotero.get(`${group.user_or_group_prefix}/items?since=${lmv[group.user_or_group_prefix]}&format=versions&includeTrashed=1`))
    if (config.zotero.limit) items.splice(config.zotero.limit, items.length)
    bar = new progress.Bar({ format: bar_format('items:add/update', prompt_width) })
    bar.start(items.length, 0)
    while (items.length) {
      logger.log('debug', `fetch: ${items.length}`)

      const batch = {
        fetched: (await zotero.get(`${group.user_or_group_prefix}/items?itemKey=${items.splice(0, zotero_batch_fetch).join(',')}&includeTrashed=1`)).map(item => item.data),
        items: [],
      }

      logger.log('debug', `batch = ${JSON.stringify(batch.fetched.map(item => ({key: item.key, type: item.itemType, parent: item.parentItem })))}`)

      for (const item of batch.fetched) {
        const row: { [key: string]: string | boolean | string[] } = {
          key: item.key,
          user_or_group_prefix: group.user_or_group_prefix,
          deleted: !!item.deleted,
          item_type: item.itemType,
          tags: (item.tags || []).filter(tag => tag.type !== 1).map(tag => tag.tag),
          automatic_tags: (item.tags || []).filter(tag => tag.type === 1).map(tag => tag.tag),
          collections: item.collections || [],
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

            break

          default:
            for (const [alias, field] of Object.entries(fieldAlias)) {
              if (item[alias]) item[field] = item[alias]
            }
            for (const field of item_fields) {
              row[field2column(field)] = item[field] || null
            }
            row.creators = (item.creators || []).map(c => [c.name, c.lastName, c.firstName].filter(n => n).join(', '))

            break
        }

        batch.items.push(row)
      }

      save.add(db.query(insert_items, [JSON.stringify(batch.items)]), batch.items.length)

      bar.update(bar.total - items.length)
    }
    bar.update(bar.total)
    bar.stop()

    await save.start()

    console.log('Marking deleted items')
    // do it twice to capture notes-on-attachments
    await db.query('UPDATE sync.items SET deleted = TRUE WHERE parent_item in (SELECT "key" FROM sync.items WHERE deleted)')
    await db.query('UPDATE sync.items SET deleted = TRUE WHERE parent_item in (SELECT "key" FROM sync.items WHERE deleted)')

    console.log('saving')
    await db.query(`
      INSERT INTO sync.lmv (user_or_group_prefix, version, name)
      VALUES ($1, $2, $3)
      ON CONFLICT (user_or_group_prefix) DO UPDATE SET version = $2, name = $3
    `, [ group.user_or_group_prefix, zotero.lmv, group.name ])

    try {
      await db.query('COMMIT')
    } catch (err) {
      console.log('sync failed:', err.message)
      logger.error(`sync failed: ${err.message}`)
    }
  }

  await db.query('BEGIN')

  console.log('populating view')
  await db.query('DROP MATERIALIZED VIEW IF EXISTS public.items')

  const view = `
    CREATE MATERIALIZED VIEW public.items AS

    WITH RECURSIVE parent_collections("key", parent_collection) AS ( -- find all ancestors of all collections
      SELECT c."key", c.parent_collection
      FROM sync.collections AS c

      UNION ALL

      SELECT sc."key", p.parent_collection
      FROM parent_collections AS p
      JOIN sync.collections as sc on sc.parent_collection = p."key"
      WHERE p.parent_collection is not null
    )
    ,
    all_collections("key", name) AS ( -- find all names for the collections
      SELECT "key", "name" FROM sync.collections

      UNION

      SELECT c."key", p.name FROM parent_collections c JOIN sync.collections p ON p."key" = c.parent_collection
    )
    ,
    all_collections_and_group ("key", "name") AS ( -- add the group name
      SELECT i."key", ac.name
      FROM sync.items i
      JOIN all_collections ac ON ac."key" = ANY(i.collections)

      UNION

      SELECT i."key", lmv.name
      FROM sync.items i
      JOIN sync.lmv ON i.user_or_group_prefix = lmv.user_or_group_prefix
    )
    ,
    all_collections_as_arrays ("key", "collections") AS ( -- group the names into an array column
      SELECT i."key", array_agg(ac.name ORDER BY name)
      FROM sync.items i
      JOIN all_collections_and_group ac ON ac."key" = i."key"
      GROUP BY i."key"
    ),

    notes AS (
      SELECT parent_item, string_agg(abstract_note, '\\n' ORDER BY "key") as notes
      FROM sync.items
      WHERE parent_item IS NOT NULL AND item_type = 'note' AND NOT deleted
      GROUP BY parent_item
    ),

    SELECT
      i.user_or_group_prefix,
      sync.lmv.name,
      array_to_string(i.creators, ', ') as creators,
      c.name AS collection,
      array_to_string(ac.collections, ', ') as parent_collections,
      tag,
      automatic_tag,
      notes.notes,
      ${item_columns.join(', ')}
    FROM sync.items i
    JOIN sync.lmv lmv ON i.user_or_group_prefix = lmv.user_or_group_prefix
    LEFT JOIN all_collections_as_arrays ac ON ac."key" = i."key"
    LEFT JOIN sync.collections c ON c."key" = ANY(i.collections)
    LEFT JOIN LATERAL unnest(tags) AS _tags(tag) ON TRUE
    LEFT JOIN LATERAL unnest(automatic_tags) AS _automatic_tags(automatic_tag) ON TRUE
    LEFT JOIN notes ON notes.parent_item = "key"
    WHERE item_type NOT IN ('attachment', 'note') AND NOT deleted
  `
  await db.query(view)

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

  config.db.index = config.db.index.split(',').map(col => col.trim()).filter(col => col)
  for (const always of ['tag', 'automatic_tag', 'collection']) {
    if (!config.db.index.includes(always)) config.db.index.push(always)
  }

  let shown = false
  for (const col of config.db.index) {
    if (!columns.includes(col)) {
      if (!shown) console.log(`available for index: ${columns.join(', ')}`)
      console.log(`not indexing unknown column ${col}`)
      shown = true
    } else {
      console.log('indexing', col)
      await db.query(`CREATE INDEX IF NOT EXISTS items_${col} ON public.items("${col}")`)
    }
  }

  console.log(`sync version=${version_to_int(pkg.version)}`)
  await db.query('INSERT INTO sync.lmv (id, version) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET version = $2', ['sync', version_to_int(pkg.version)])

  console.log('refreshing view')
  await db.query('REFRESH MATERIALIZED VIEW public.items')
  await db.query('COMMIT')

  for (const count of (await db.query('SELECT COUNT(*) AS items from sync.items')).rows) {
    console.log(`total items: ${count.items}`)
  }
  for (const count of (await db.query('SELECT COUNT(*) AS items from sync.items WHERE NOT deleted')).rows) {
    console.log(`non-deleted items: ${count.items}`)
  }
  for (const count of (await db.query('SELECT COUNT(*) AS items from public.items')).rows) {
    console.log(`items in view: ${count.items}`)
  }
})
