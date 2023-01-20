// Copyright (C) 2021 Michael J. Fromberger. All Rights Reserved.

// Package bitcaskstore implements the blob.Store interface on a bitcask
// database.
package bitcaskstore

import (
	"context"

	"git.mills.io/prologic/bitcask"
	"github.com/creachadair/ffs/blob"
)

// Opener constructs a bitcaskstore from a path address.
func Opener(_ context.Context, addr string) (blob.Store, error) {
	return Open(addr, nil)
}

// Store implements the blob.Store interface on a bitcask database.
type Store struct {
	db *bitcask.Bitcask
}

// Open opens a store for a bitcask database at the specified path.
// If opts == nil, default settings are used as described on Options.
func Open(path string, opts *Options) (*Store, error) {
	db, err := bitcask.Open(path)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	bits, err := s.db.Get([]byte(key))
	if err == bitcask.ErrKeyNotFound || err == bitcask.ErrEmptyKey {
		return nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, err
	}
	return bits, nil
}

// Put implements part of blob.Store. The bitcask implementation does not
// accept empty keys, so Put will report blob.ErrKeyNotFound for that case.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
	if opts.Key == "" {
		return blob.ErrKeyNotFound // bitcask does not accept empty keys
	}
	bkey := []byte(opts.Key)
	if !opts.Replace && s.db.Has(bkey) {
		return blob.KeyExists(opts.Key)
	}
	return s.db.Put(bkey, opts.Data)
}

// Delete implements part of blob.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	bkey := []byte(key)
	if !s.db.Has(bkey) {
		return blob.KeyNotFound(key)
	}
	return s.db.Delete(bkey)
}

// Size implements part of blob.Store.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	bits, err := s.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	return int64(len(bits)), nil
}

// List implements part of blob.Store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	// N.B. Bitcask's Scan is a true prefix scan, so we can't use start as a
	// prefix or we will not get any keys later in the sequence. The db provides
	// a Range query, but no way to determine the end of the available range, so
	// we have to just read the whole thing and filter it.
	//
	// Moreover, while a scan is running, the database is locked, so it is not
	// possible to update whie the scan is running. To mitigate this, we
	// implement List as a sequence of scans across the possible range of keys,
	// and buffer chunks of them to process outside the lock.

	i := 0
	if len(start) != 0 {
		i = int(start[0])
	}
	for ; i < 256; i++ {
		var keys []string
		if err := s.db.Scan([]byte{byte(i)}, func(key []byte) error {
			if s := string(key); s >= start {
				keys = append(keys, string(key))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, key := range keys {
			if err := f(key); err == blob.ErrStopListing {
				return nil
			} else if err != nil {
				return err
			}
		}
	}
	return nil
}

// Len implements part of blob.Store. This implementation never returns an error.
func (s *Store) Len(ctx context.Context) (int64, error) { return int64(s.db.Len()), nil }

// Close implements part of the blob.Store interface. It syncs and closes all
// of the data files in use by the database.
func (s *Store) Close(_ context.Context) error { return s.db.Close() }

// Options are configurations for a Store. A nil *Options is ready for use and
// provides default values as described.
type Options struct{}
