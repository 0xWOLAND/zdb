const std = @import("std");
const zdb = @import("zdb");
const Pager = zdb.Pager;

const TestTree = zdb.BTree(i32, []const u8, 4096, 0);

fn cmp(a: i32, b: i32) callconv(.Inline) i32 {
    return if (a < b) -1 else if (a > b) 1 else 0;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const db_file = "test.db";
    defer std.fs.cwd().deleteFile(db_file) catch {};

    var pager = try Pager.init(allocator, db_file);
    defer pager.deinit();

    var tree = try TestTree.init(allocator, &pager);

    // Start transaction
    try tree.beginTx();

    // Basic test
    try tree.put(1, "one", cmp);
    try tree.put(2, "two", cmp);
    try tree.put(3, "three", cmp);

    // Commit
    try tree.commitTx();

    std.debug.assert(std.mem.eql(u8, tree.get(1, cmp).?, "one"));
    std.debug.assert(std.mem.eql(u8, tree.get(2, cmp).?, "two"));
    std.debug.assert(std.mem.eql(u8, tree.get(3, cmp).?, "three"));
    std.debug.assert(tree.get(4, cmp) == null);

    // Update test
    try tree.beginTx();
    try tree.put(2, "TWO", cmp);
    try tree.commitTx();

    std.debug.assert(std.mem.eql(u8, tree.get(2, cmp).?, "TWO"));

    // Larger test (trigger splits)
    try tree.beginTx();
    for (0..800) |i| {
        const key: i32 = @intCast(i);
        const value = try std.fmt.allocPrint(allocator, "v{d}", .{i});
        defer allocator.free(value);
        try tree.put(key, value, cmp);
    }
    try tree.commitTx();

    // Verify samples
    std.debug.assert(tree.get(0, cmp) != null);
    std.debug.assert(tree.get(400, cmp) != null);
    std.debug.assert(tree.get(799, cmp) != null);

    std.debug.print("✓ All tests passed!\n", .{});
}
