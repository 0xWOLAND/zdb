demo:
	zig build demo -Doptimize=ReleaseFast

bench:
	zig build bench -Doptimize=ReleaseFast

test:
	zig build test
