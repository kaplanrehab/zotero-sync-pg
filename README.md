# Install

* install [NodeJS 10+](https://nodejs.org/en/)
* run `npm install`
* Get a Zotero API key [here](https://www.zotero.org/settings/keys/new) -- give it *read only* access to what you want to have synced
* copy `config.ini.sample` to `config.ini` and fill it out


# Sync

Run `npm start` after changes to your Zotero library. First sync might take very long depending on the size of your library.

When sync is done, you can select from `public.items` and `public.items_bundled_autotags`, which are views over the synced data.

For a full reset, run `npm run reset`.
