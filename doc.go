// Package memcached provides a thread-safe memcached text+meta client.
//
// Application commands:
//   - set/add/replace/append/prepend/cas
//   - get/gets/gat/gats
//   - ms/mg/md/mn/ma/me
//   - delete, incr/decr, touch
//   - version, stats (and StatsArgs)
//   - flush_all (opt-in via WithAllowFlushAll)
//
// Admin commands (verbosity, shutdown, slabs, lru*, watch, crawler dumps)
// are available only through Client.Admin after WithAdminCommands.
//
// Values are opaque []byte. Serialize or compress in the caller before Set.
//
// Cluster mode defaults to CRC-32 IEEE modulo N (gomemcache-compatible, not
// Ketama). Use NewKetamaPickBuilder for libmemcached/php-memcached interop.
// murmur3 and rendezvous are also available. Mixing the default picker with
// php-memcached on the same cluster places keys on different nodes.
package memcached
