test:
	zig run -O ReleaseFast test_btree.zig

bench:
	zig run -O ReleaseFast benchmark.zig
