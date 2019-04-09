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
import * as semver from 'semver'
import * as colors from 'colors/safe'

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
      console.log(colors.red('main failed:'), err.stack)
      logger.error(err)
      process.exit(1)
    })
}

class Zotero {
  public lmv: number
  public userID: number
  public api_key: string
  public groups: { [key: string]: { user_or_group_prefix: string, name: string } } = {}

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

class ParallelSaver {
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

async function list_views(db) {
  for (const view of (await db.query('select table_schema, table_name from INFORMATION_SCHEMA.views WHERE table_schema = ANY(current_schemas(false))')).rows) {
    console.log(`view: ${view.table_schema}.${view.table_name}`)
  }
}

class View {
  public name: string

  private db: any
  private item_columns: string[]
  private bundle_auto_tags: boolean

  constructor(db, name, item_columns) {
    this.db = db
    this.name = name
    this.item_columns = item_columns
    this.bundle_auto_tags = (name === 'items_bundled_autotags')
  }

  public async columns() {
    const _columns = `
      SELECT mv.attname as col
      FROM pg_attribute mv
      JOIN pg_class t on mv.attrelid = t.oid
      JOIN pg_namespace s on t.relnamespace = s.oid
      WHERE mv.attnum > 0
        AND NOT mv.attisdropped
        AND t.relname = $1
        AND s.nspname = 'public'
    `

    return (await this.db.query(_columns, [this.name])).rows.map(col => col.col)
  }

  public async create() {
    console.log(`populating view ${this.name}`)
    await this.db.query(`DROP MATERIALIZED VIEW IF EXISTS public.${this.name}`)

    let automatic_tags_field, automatic_tags_join, sep

    if (this.bundle_auto_tags) {
      switch ((config.delimiter.automatic_tags || 'comma').toLowerCase()) {
        case 'comma':
          sep = "', '"
          break

        case 'cr':
          sep = "E'\\n'"
          break

        case 'crcr':
          sep = "E'\\n\\n'"
          break

        default:
          throw new Error(`Unexpected delimiter ${JSON.stringify(config.delimiter.automatic_tags)} for config.delimiter.automatic_tags`)
      }

      automatic_tags_field = `array_to_string(i.automatic_tags, ${sep}) as automatic_tag`
      automatic_tags_join = ''

    } else {
      automatic_tags_field = 'automatic_tag'
      automatic_tags_join = 'LEFT JOIN LATERAL unnest(automatic_tags) AS _automatic_tags(automatic_tag) ON TRUE'

    }

    const view = `
      CREATE MATERIALIZED VIEW public.${this.name} AS

      WITH RECURSIVE parent_collections("key", name, parent_collection, path) AS (
        SELECT c."key", c.name, c.parent_collection, (', ' || c.name::TEXT) AS path
        FROM sync.collections AS c
        WHERE c.parent_collection IS NULL

        UNION ALL

        SELECT c."key", c.name, c.parent_collection, (p.path || ', ' || c.name::TEXT)
        FROM parent_collections AS p, sync.collections AS c
        WHERE c.parent_collection = p."key"
      ),

      notes AS (
        SELECT parent_item, string_agg(abstract_note, E'\\n' ORDER BY "key") as notes
        FROM sync.items
        WHERE parent_item IS NOT NULL AND item_type = 'note' AND NOT deleted
        GROUP BY parent_item
      )

      SELECT
        i.user_or_group_prefix,
        lmv.name,
        array_to_string(i.creators, ', ') as creators,
        c.name AS collection,
        (lmv.name || COALESCE(pc.path, '')) as parent_collections,
        tag,
        ${automatic_tags_field},
        notes.notes,
        ${this.item_columns.join(', ')}
      FROM sync.items i
      JOIN sync.lmv lmv ON i.user_or_group_prefix = lmv.user_or_group_prefix
      LEFT JOIN sync.collections c ON c."key" = ANY(i.collections)
      LEFT JOIN parent_collections pc on c."key" = pc."key"
      LEFT JOIN LATERAL unnest(tags) AS _tags(tag) ON TRUE
      ${automatic_tags_join}
      LEFT JOIN notes ON notes.parent_item = i."key"
      WHERE item_type NOT IN ('attachment', 'note') AND NOT deleted
    `
    await this.db.query(view)

    const index_columns = ['tag', 'automatic_tag', 'collection']
    for (const col of config.db.index) {
      if (!index_columns.includes(col)) index_columns.push(col)
    }

    const view_columns = await this.columns()
    let shown = false
    for (const col of index_columns) {
      if (!view_columns.includes(col)) {
        if (!shown) console.log(`available for index on ${this.name}: ${view_columns.join(', ')}`)
        console.log(`not indexing unknown column ${this.name}.${col}`)
        shown = true
      } else {
        console.log(`indexing ${this.name}.${col}`)
        await this.db.query(`CREATE INDEX IF NOT EXISTS items_${col} ON public.${this.name}("${col}")`)
      }
    }

    console.log(`refreshing view ${this.name}`)
    await this.db.query(`REFRESH MATERIALIZED VIEW public.${this.name}`)
    await list_views(this.db)
  }
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

  const version = (await db.query(`
    SELECT CASE
      WHEN to_regclass($1) IS NULL THEN $2
      ELSE COALESCE(pg_catalog.obj_description(to_regclass($1), 'pg_class'), $2)
    END AS version
  `, ['sync.lmv', '0.0.0'])).rows[0].version
  if (semver.lt(pkg.version, version)) {
    console.log(colors.red(`You are running ${pkg.version}, database was synced with version ${version}, please upgrade your scripts`))
    process.exit(1)
  }

  await db.query(`CREATE TABLE IF NOT EXISTS sync.lmv (
      user_or_group_prefix VARCHAR(20) PRIMARY KEY,
      name VARCHAR NOT NULL,
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
    CREATE TABLE IF NOT EXISTS sync.collections (
      "key" VARCHAR(8) NOT NULL,
      user_or_group_prefix VARCHAR(20) NOT NULL REFERENCES sync.lmv(user_or_group_prefix) DEFERRABLE INITIALLY DEFERRED,
      "parent_collection" VARCHAR(8),
      "name" VARCHAR
    )
  `)
  await db.query('CREATE INDEX IF NOT EXISTS collections_index_key ON sync.collections("key")')
  await db.query('CREATE INDEX IF NOT EXISTS collections_index_parent_key ON sync.collections(parent_collection)')

  const insert_items = make_insert('item', ['parentItem', 'itemType', 'user_or_group_prefix', 'deleted', 'creators', 'collections', 'tags', 'automatic_tags'].concat(item_fields))

  logger.log('debug', `syncing ${Object.values(zotero.groups).map(g => g.name).join(', ')}`)

  // remove groups we no longer have access to
  await db.query('DELETE FROM sync.items WHERE NOT (user_or_group_prefix = ANY($1))', [ Object.values(zotero.groups).map(g => g.user_or_group_prefix) ])
  await db.query('DELETE FROM sync.collections WHERE NOT (user_or_group_prefix = ANY($1))', [ Object.values(zotero.groups).map(g => g.user_or_group_prefix) ])
  await db.query('DELETE FROM sync.lmv WHERE NOT (user_or_group_prefix = ANY($1))', [ Object.values(zotero.groups).map(g => g.user_or_group_prefix) ])

  for (const group of (await db.query('select user_or_group_prefix, version from sync.lmv')).rows) {
    lmv[group.user_or_group_prefix] = group.version
  }

  console.log(colors.green('Syncing libraries and groups'))
  for (const group of Object.values(zotero.groups)) {
    console.log(colors.yellow(group.name))

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

    const save = new ParallelSaver

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
          tags: (item.tags || []).filter(tag => tag.type !== 1).map(tag => tag.tag).sort(),
          automatic_tags: (item.tags || []).filter(tag => tag.type === 1).map(tag => tag.tag).sort(),
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

        if (!row.url) {
          const identifiers = {
            pmid: '',
            pmcid: '',
            doi: item.DOI,
          }
          for (const line of (item.extra || '').split('\n')) {
            const m = line.match(/^(PMC?ID):\s+([^\s]+)/i)
            if (m) identifiers[m[1].toLowerCase()] = m[2]
          }

          if (identifiers.pmcid) {
            if (!identifiers.pmcid.startsWith('http')) identifiers.pmcid = `https://www.ncbi.nlm.nih.gov/pmc/articles/${identifiers.pmcid}/`
            row.url = identifiers.pmcid

          } else if (identifiers.doi) {
            if (!identifiers.doi.startsWith('http')) identifiers.doi = `https://doi.org/${identifiers.doi}`
            row.url = identifiers.doi

          } else if (identifiers.pmid) {
            if (!identifiers.pmid.startsWith('http')) identifiers.pmid = `https://www.ncbi.nlm.nih.gov/pubmed/${identifiers.pmid}`
            row.url = identifiers.pmid
          }
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

  console.log(colors.green('Creating views'))
  config.db.index = config.db.index.split(',').map(col => col.trim()).filter(col => col)
  const views = {}
  for (const view of ['items', 'items_bundled_autotags']) {
    console.log(colors.yellow(view))
    views[view] = new View(db, view, item_columns)
    await views[view].create()
  }

  console.log(colors.green('Done'))
  console.log(`sync version=${pkg.version}`)
  // parameter is not accepted here?
  // await db.query('COMMENT ON TABLE sync.lmv IS $1', [pkg.version])
  await db.query(`COMMENT ON TABLE sync.lmv IS '${prepareValue(pkg.version)}'`)

  await db.query('COMMIT')

  for (const count of (await db.query('SELECT COUNT(*) AS items from sync.items')).rows) {
    console.log(`total items: ${count.items}`)
  }
  for (const count of (await db.query('SELECT COUNT(*) AS items from sync.items WHERE NOT deleted')).rows) {
    console.log(`non-deleted items: ${count.items}`)
  }

  await list_views(db)

  for (const view of Object.values(views) as View[]) {
    console.log(`view ${view.name} has columns ${await view.columns()}`)
    for (const count of (await db.query(`SELECT COUNT(*) AS items from public.${view.name}`)).rows) {
      console.log(`items in view: ${count.items}`)
    }
  }

})
