# Mattress - an exploration into database internals
I am learning about database internals, this repo contains by experiments

## Current implementation
A very bare bones key value store with a disk backed write only log and in memory index

## Roadmap
- [ ] make storage page based
- [ ] implement a page cache
- [ ] allow records to span multiple pages
- [ ] implement WAL
- [ ] implement B+tree index
- [ ] ensure concurrent safety
