const std = @import("std");
const posix = std.posix;
const fs = std.fs;
const mem = std.mem;

const page_size_min = std.heap.page_size_min;
pub const PAGE_SIZE = page_size_min;
const MAGIC = 0x5A444221; // "ZDB!"
const VERSION = 1;

const MetaPage = extern struct {
    magic: u32,
    version: u32,
    page_size: u32,
    page_count: u32,
    free_list_head: u32,
    root_page: u32,
    tx_id: u64,
    _reserved: [PAGE_SIZE - 32]u8 = undefined,
};

const PageHeader = extern struct {
    flags: u16,
    overflow: u16,
    lower: u16,
    upper: u16,
};

const PageFlags = struct {
    const BRANCH = 0x01;
    const LEAF = 0x02;
    const OVERFLOW = 0x04;
    const META = 0x08;
    const DIRTY = 0x10;
};

pub const Pager = struct {
    file: fs.File,
    map: []align(page_size_min) u8,
    meta: *MetaPage,
    page_count: u32,
    allocator: mem.Allocator,
    dirty_pages: std.AutoHashMap(u32, []u8),
    tx_active: bool,

    pub fn init(allocator: mem.Allocator, path: []const u8) !Pager {
        const file = try fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
            .lock = .exclusive,
        });
        errdefer file.close();

        const file_size = try file.getEndPos();
        const initial_size = if (file_size == 0) PAGE_SIZE * 16 else file_size;

        if (file_size == 0) {
            try file.setEndPos(initial_size);
            
            var meta = MetaPage{
                .magic = MAGIC,
                .version = VERSION,
                .page_size = PAGE_SIZE,
                .page_count = @intCast(initial_size / PAGE_SIZE),
                .free_list_head = 0,
                .root_page = 0,
                .tx_id = 0,
            };
            
            try file.pwriteAll(mem.asBytes(&meta), 0);
        }

        const map = try posix.mmap(
            null,
            initial_size,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .PRIVATE },
            file.handle,
            0
        );

        const pager = Pager{
            .file = file,
            .map = map,
            .meta = @ptrCast(@alignCast(map.ptr)),
            .page_count = @intCast(initial_size / PAGE_SIZE),
            .allocator = allocator,
            .dirty_pages = std.AutoHashMap(u32, []u8).init(allocator),
            .tx_active = false,
        };

        if (pager.meta.magic != MAGIC) {
            return error.InvalidDatabase;
        }

        return pager;
    }

    pub fn deinit(self: *Pager) void {
        var iter = self.dirty_pages.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        self.dirty_pages.deinit();
        posix.munmap(self.map);
        self.file.close();
    }

    pub fn beginTx(self: *Pager) !void {
        if (self.tx_active) return error.TransactionActive;
        self.tx_active = true;
    }

    pub fn commitTx(self: *Pager) !void {
        if (!self.tx_active) return error.NoActiveTransaction;
        
        var iter = self.dirty_pages.iterator();
        while (iter.next()) |entry| {
            const page_id = entry.key_ptr.*;
            const data = entry.value_ptr.*;
            const offset = page_id * PAGE_SIZE;
            try self.file.pwriteAll(data, offset);
            
            // Also update the mmap'd memory so reads see the new data
            @memcpy(self.map[offset..offset + PAGE_SIZE], data);
        }

        try self.file.sync();

        self.meta.tx_id += 1;
        const meta_bytes = mem.asBytes(self.meta);
        try self.file.pwriteAll(meta_bytes[0..PAGE_SIZE], 0);
        try self.file.sync();

        iter = self.dirty_pages.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        self.dirty_pages.clearRetainingCapacity();
        
        self.tx_active = false;
    }

    pub fn rollbackTx(self: *Pager) void {
        if (!self.tx_active) return;
        
        var iter = self.dirty_pages.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        self.dirty_pages.clearRetainingCapacity();
        
        self.tx_active = false;
    }

    pub fn getPage(self: *Pager, page_id: u32) ![]const u8 {
        if (page_id >= self.page_count) {
            return error.PageOutOfBounds;
        }

        if (self.dirty_pages.get(page_id)) |page| {
            return page;
        }

        const offset = page_id * PAGE_SIZE;
        return self.map[offset..offset + PAGE_SIZE];
    }

    pub fn getPageForWrite(self: *Pager, page_id: u32) ![]u8 {
        if (!self.tx_active) return error.NoActiveTransaction;
        if (page_id >= self.page_count) return error.PageOutOfBounds;

        if (self.dirty_pages.get(page_id)) |page| {
            return page;
        }

        const cow_page = try self.allocator.alloc(u8, PAGE_SIZE);
        errdefer self.allocator.free(cow_page);

        const offset = page_id * PAGE_SIZE;
        @memcpy(cow_page, self.map[offset..offset + PAGE_SIZE]);

        try self.dirty_pages.put(page_id, cow_page);
        return cow_page;
    }

    pub fn allocPage(self: *Pager) !u32 {
        if (!self.tx_active) return error.NoActiveTransaction;

        if (self.meta.free_list_head != 0) {
            const page_id = self.meta.free_list_head;
            const page = try self.getPageForWrite(page_id);
            self.meta.free_list_head = @as(*u32, @ptrCast(@alignCast(page.ptr))).*;
            return page_id;
        }

        if (self.page_count >= self.meta.page_count) {
            const new_size = self.meta.page_count * 2;
            try self.file.setEndPos(new_size * PAGE_SIZE);
            
            posix.munmap(self.map);
            self.map = try posix.mmap(
                null,
                new_size * PAGE_SIZE,
                posix.PROT.READ | posix.PROT.WRITE,
                .{ .TYPE = .PRIVATE },
                self.file.handle,
                0
            );
            
            self.meta = @ptrCast(@alignCast(self.map.ptr));
            self.meta.page_count = new_size;
        }

        const page_id = self.page_count;
        self.page_count += 1;
        
        const page = try self.getPageForWrite(page_id);
        @memset(page, 0);
        
        return page_id;
    }

    pub fn freePage(self: *Pager, page_id: u32) !void {
        if (!self.tx_active) return error.NoActiveTransaction;
        if (page_id == 0) return error.CannotFreeMetaPage;
        
        const page = try self.getPageForWrite(page_id);
        @as(*u32, @ptrCast(@alignCast(page.ptr))).* = self.meta.free_list_head;
        self.meta.free_list_head = page_id;
    }

    pub fn grow(self: *Pager, new_page_count: u32) !void {
        if (new_page_count <= self.page_count) return;
        
        const new_size = new_page_count * PAGE_SIZE;
        try self.file.setEndPos(new_size);
        
        posix.munmap(self.map);
        self.map = try posix.mmap(
            null,
            new_size,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .PRIVATE },
            self.file.handle,
            0
        );
        
        self.meta = @ptrCast(@alignCast(self.map.ptr));
        self.page_count = new_page_count;
        self.meta.page_count = new_page_count;
    }
};

test "pager basic operations" {
    const allocator = std.testing.allocator;
    
    const test_file = "test.db";
    defer std.fs.cwd().deleteFile(test_file) catch {};
    
    var pager = try Pager.init(allocator, test_file);
    defer pager.deinit();
    
    try pager.beginTx();
    
    const page_id = try pager.allocPage();
    const page = try pager.getPageForWrite(page_id);
    
    const data = "Hello, World!";
    @memcpy(page[0..data.len], data);
    
    try pager.commitTx();
    
    const read_page = try pager.getPage(page_id);
    try std.testing.expectEqualStrings(data, read_page[0..data.len]);
}

test "pager copy-on-write" {
    const allocator = std.testing.allocator;
    
    const test_file = "test_cow.db";
    defer std.fs.cwd().deleteFile(test_file) catch {};
    
    var pager = try Pager.init(allocator, test_file);
    defer pager.deinit();
    
    try pager.beginTx();
    const page_id = try pager.allocPage();
    const page1 = try pager.getPageForWrite(page_id);
    @memcpy(page1[0..4], "ORIG");
    try pager.commitTx();
    
    const original = try pager.getPage(page_id);
    try std.testing.expectEqualStrings("ORIG", original[0..4]);
    
    try pager.beginTx();
    const page2 = try pager.getPageForWrite(page_id);
    @memcpy(page2[0..4], "NEW!");
    
    const still_original = try pager.getPage(page_id);
    try std.testing.expectEqualStrings("NEW!", still_original[0..4]);
    
    pager.rollbackTx();
    
    const after_rollback = try pager.getPage(page_id);
    try std.testing.expectEqualStrings("ORIG", after_rollback[0..4]);
}