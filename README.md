# Mattress - an exploration into database internals
I am learning about database internals, this repo contains by experiments

## Current implementation
A very bare bones key value store with a disk backed slotted pages

## Roadmap
- [x] make storage page based
- [x] implement a page cache (buffer pool)
- [ ] internal transactions (currently there is a chance to lose data when moving a record to a new page)
- [ ] allow records to span multiple pages
- [ ] implement WAL
- [ ] implement B+tree index
- [ ] ensure concurrent safety
