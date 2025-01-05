// Copyright (C) 2021 Michael J. Fromberger. All Rights Reserved.

// Package bitcaskstore implements the [blob.StoreCloser] interface on a
// bitcask database.
package bitcaskstore

import (
	"context"
	"errors"
	"iter"

	"git.mills.io/prologic/bitcask"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/monitor"
)

// Opener constructs a bitcaskstore from a path address.
func Opener(_ context.Context, addr string) (blob.StoreCloser, error) {
	return New(addr, nil)
}

// Store implements the [blob.StoreCloser] interface using a Bitcask database.
type Store struct {
	*monitor.M[*bitcask.Bitcask, KV]
}

// New constructs a Store by opening or creating a Bitcask database with the
// specified path and options.
func New(path string, opts *Options) (Store, error) {
	db, err := bitcask.Open(path)
	if err != nil {
		return Store{}, err
	}
	return Store{M: monitor.New(monitor.Config[*bitcask.Bitcask, KV]{
		DB:     db,
		Prefix: opts.keyPrefix(),
		NewKV: func(_ context.Context, db *bitcask.Bitcask, pfx dbkey.Prefix, _ string) (KV, error) {
			return KV{db: db, prefix: pfx}, nil
		},
	})}, nil
}

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(_ context.Context) error { return s.DB.Close() }

// KV implements the [blob.KV] interface on a bitcask database.
type KV struct {
	db     *bitcask.Bitcask
	prefix dbkey.Prefix
}

// Get implements part of [blob.KV].
func (s KV) Get(ctx context.Context, key string) ([]byte, error) {
	realKey := []byte(s.prefix.Add(key))
	bits, err := s.db.Get(realKey)
	if errors.Is(err, bitcask.ErrKeyNotFound) || errors.Is(err, bitcask.ErrEmptyKey) {
		return nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, err
	}
	return bits, nil
}

// Has implements part of [blob.KV].
func (s KV) Has(ctx context.Context, keys ...string) (blob.KeySet, error) {
	var out blob.KeySet
	for _, key := range keys {
		_, err := s.db.Get([]byte(s.prefix.Add(key)))
		if errors.Is(err, bitcask.ErrKeyNotFound) || errors.Is(err, bitcask.ErrEmptyKey) {
			continue
		} else if err != nil {
			return nil, err
		}
		out.Add(key)
	}
	return out, nil
}

// Put implements part of [blob.KV]. The bitcask implementation does not accept
// empty keys, so Put will report [blob.ErrKeyNotFound] for that case.
func (s KV) Put(ctx context.Context, opts blob.PutOptions) error {
	if opts.Key == "" {
		return blob.ErrKeyNotFound // bitcask does not accept empty keys
	}
	bkey := []byte(s.prefix.Add(opts.Key))
	if !opts.Replace && s.db.Has(bkey) {
		return blob.KeyExists(opts.Key)
	}
	return s.db.Put(bkey, opts.Data)
}

// Delete implements part of [blob.KV].
func (s KV) Delete(ctx context.Context, key string) error {
	bkey := []byte(s.prefix.Add(key))
	if !s.db.Has(bkey) {
		return blob.KeyNotFound(key)
	}
	return s.db.Delete(bkey)
}

var errStopListing = errors.New("stop listing")

// List implements part of [blob.KV].
func (s KV) List(ctx context.Context, start string) iter.Seq2[string, error] {
	// N.B. Bitcask's Scan is a true prefix scan, so we can't use start as a
	// prefix or we will not get any keys later in the sequence. The db provides
	// a Range query, but no way to determine the end of the available range, so
	// we have to just read the whole thing and filter it.
	//
	// Moreover, while a scan is running, the database is locked, so it is not
	// possible to update whie the scan is running. To mitigate this, we
	// implement List as a sequence of scans across the possible range of keys,
	// and buffer chunks of them to process outside the lock.
	return func(yield func(string, error) bool) {
		i := 0
		if len(start) != 0 {
			i = int(start[0])
		}
		for ; i < 256; i++ {
			shard := []byte(s.prefix.Add(string(byte(i))))
			err := s.db.Scan(shard, func(key []byte) error {
				observed := s.prefix.Remove(string(key))
				if observed >= start {
					if !yield(observed, nil) {
						return errStopListing
					}
				}
				return nil
			})
			if err == errStopListing {
				return
			} else if err != nil {
				yield("", err)
				return
			}
		}
	}
}

// Len implements part of [blob.KV].
func (s KV) Len(ctx context.Context) (n int64, err error) {
	err = s.db.Scan([]byte(s.prefix), func([]byte) error {
		n++
		return nil
	})
	return
}

// Close implements part of the [blob.KV] interface. It syncs and closes all of
// the data files in use by the database.
func (s KV) Close(_ context.Context) error { return s.db.Close() }

// Options are configurations for a [KV] or a [Store]. A nil *Options is ready
// for use and provides default values as described.
type Options struct {
	// KeyPrefix, if set, restricts access to keys beginning with this prefix.
	KeyPrefix string
}

func (o *Options) keyPrefix() dbkey.Prefix {
	if o == nil {
		return ""
	}
	return dbkey.Prefix(o.KeyPrefix)
}
