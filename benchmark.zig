const std = @import("std");
const Pager = @import("src/pager.zig").Pager;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const n = 10000;
    var timer = try std.time.Timer.start();

    // In-memory
    var mem = try allocator.alloc([2]i32, n);
    defer allocator.free(mem);

    for (0..n) |i| mem[i] = .{ @intCast(i), @intCast(i * 2) };
    const mem_write = timer.lap();

    var sum: i64 = 0;
    for (0..n) |i| sum += mem[i][1];
    const mem_read = timer.lap();

    // Pager
    const db = "bench.db";
    defer std.fs.cwd().deleteFile(db) catch {};
    var pager = try Pager.init(allocator, db);
    defer pager.deinit();

    var pages = try allocator.alloc(u32, n);
    defer allocator.free(pages);

    try pager.beginTx();
    for (0..n) |i| {
        const p = try pager.allocPage();
        pages[i] = p;
        const data = try pager.getPageForWrite(p);
        @as(*[2]i32, @ptrCast(@alignCast(data.ptr))).* = .{ @intCast(i), @intCast(i * 2) };
    }
    try pager.commitTx();
    const pager_write = timer.lap();

    sum = 0;
    for (pages) |p| {
        const data = try pager.getPage(p);
        sum += @as(*const [2]i32, @ptrCast(@alignCast(data.ptr))).*[1];
    }
    const pager_read = timer.lap();

    std.debug.print("Memory: write {d:.0}ns/op, read {d:.0}ns/op\n", .{
        @as(f64, @floatFromInt(mem_write)) / n,
        @as(f64, @floatFromInt(mem_read)) / n,
    });
    std.debug.print("Pager:  write {d:.0}ns/op, read {d:.0}ns/op\n", .{
        @as(f64, @floatFromInt(pager_write)) / n,
        @as(f64, @floatFromInt(pager_read)) / n,
    });
    std.debug.print("Sum: {}", .{sum});
}
