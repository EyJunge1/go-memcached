// Package memcached provides a memcached client implementation.
//
// The client is thread-safe. It supports the memcached text protocol:
//   - set/add/replace/append/prepend/cas
//   - get/gets/gat/gats
//   - ms/mg/md/mn/ma/me
//   - delete
//   - incr/decr
//   - touch
//   - version
//   - stats
//
// Values are opaque []byte. Serialize or compress in the caller before Set.
//
// Cluster mode picks a node per key with crc32, murmur3, rendezvous, or a
// custom hash.
package memcached
