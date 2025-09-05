const std = @import("std");
const Pager = @import("pager.zig").Pager;

pub fn BTree(
    comptime Key: type,
    comptime Value: type,
    comptime page_size: usize,
    comptime order_hint: usize,
) type {
    return struct {
        const Self = @This();
        const PageId = u32;
        const NULL_PAGE: PageId = 0;

        const NodeKind = enum(u8) { internal, leaf };

        const NodeHeader = extern struct {
            kind: NodeKind,
            key_count: u16,
            _pad: u8 = 0,
        };

        const key_size = @sizeOf(Key);
        const value_size = @sizeOf(Value);
        const page_id_size = @sizeOf(PageId);
        const header_size = @sizeOf(NodeHeader);

        const internal_capacity: usize = (page_size - header_size) / (key_size + page_id_size);
        const leaf_capacity: usize = (page_size - header_size - page_id_size) / (key_size + value_size);

        const ORDER_INTERNAL = if (order_hint == 0) internal_capacity else @min(order_hint, internal_capacity);
        const ORDER_LEAF = if (order_hint == 0) leaf_capacity else @min(order_hint, leaf_capacity);

        const HEADER_OFFSET = 0;
        const KEYS_OFFSET_LEAF = std.mem.alignForward(usize, header_size, @alignOf(Key));
        const VALUES_OFFSET_LEAF = std.mem.alignForward(usize, KEYS_OFFSET_LEAF + ORDER_LEAF * key_size, @alignOf(Value));
        const NEXT_LEAF_OFFSET = page_size - page_id_size;

        const KEYS_OFFSET_INTERNAL = std.mem.alignForward(usize, header_size, @alignOf(Key));
        const CHILDREN_OFFSET_INTERNAL = std.mem.alignForward(usize, KEYS_OFFSET_INTERNAL + ORDER_INTERNAL * key_size, @alignOf(PageId));

        allocator: std.mem.Allocator,
        pager: *Pager,
        root_page_id: PageId,

        pub fn init(allocator: std.mem.Allocator, pager: *Pager) !Self {
            try pager.beginTx();
            errdefer pager.rollbackTx();

            const root_id = try pager.allocPage();
            const root_data = try pager.getPageForWrite(root_id);

            const header = @as(*NodeHeader, @ptrCast(@alignCast(root_data.ptr)));
            header.* = NodeHeader{ .kind = .leaf, .key_count = 0 };

            pager.meta.root_page = root_id;
            try pager.commitTx();

            return Self{
                .allocator = allocator,
                .pager = pager,
                .root_page_id = root_id,
            };
        }

        pub fn open(allocator: std.mem.Allocator, pager: *Pager) !Self {
            const root_id = pager.meta.root_page;
            if (root_id == 0) {
                return init(allocator, pager);
            }

            return Self{
                .allocator = allocator,
                .pager = pager,
                .root_page_id = root_id,
            };
        }

        pub fn beginTx(self: *Self) !void {
            try self.pager.beginTx();
        }

        pub fn commitTx(self: *Self) !void {
            try self.pager.commitTx();
        }

        pub fn rollbackTx(self: *Self) void {
            self.pager.rollbackTx();
        }

        pub fn get(self: *const Self, key: Key, cmp: anytype) ?Value {
            var current_page = self.root_page_id;

            while (true) {
                const page_data = self.pager.getPage(current_page) catch return null;
                const header = @as(*const NodeHeader, @ptrCast(@alignCast(&page_data[HEADER_OFFSET]))).*;

                const keys_offset = if (header.kind == .leaf) KEYS_OFFSET_LEAF else KEYS_OFFSET_INTERNAL;
                const keys = @as([*]const Key, @ptrCast(@alignCast(&page_data[keys_offset])));
                const keys_slice = keys[0..header.key_count];

                if (header.key_count > 0) {
                    const prefetch_index = header.key_count >> 1;
                    @prefetch(&keys_slice[prefetch_index], .{ .rw = .read, .locality = 3, .cache = .data });
                }

                const idx = binarySearch(Key, keys_slice, key, cmp);

                switch (header.kind) {
                    .internal => {
                        const children = @as([*]const PageId, @ptrCast(@alignCast(&page_data[CHILDREN_OFFSET_INTERNAL])));
                        current_page = children[idx];
                    },
                    .leaf => {
                        if (idx < header.key_count and cmp(keys[idx], key) == 0) {
                            const values = @as([*]const Value, @ptrCast(@alignCast(&page_data[VALUES_OFFSET_LEAF])));
                            return values[idx];
                        }
                        return null;
                    },
                }
            }
        }

        pub fn put(self: *Self, key: Key, value: Value, cmp: anytype) !void {
            const root_data = try self.pager.getPage(self.root_page_id);
            const root_header = @as(*const NodeHeader, @ptrCast(@alignCast(&root_data[HEADER_OFFSET]))).*;

            const is_full = switch (root_header.kind) {
                .leaf => root_header.key_count >= ORDER_LEAF,
                .internal => root_header.key_count >= ORDER_INTERNAL,
            };

            if (is_full) {
                try self.splitRoot();
            }
            try self.insertNonFull(self.root_page_id, key, value, cmp);
        }

        fn createNode(self: *Self, kind: NodeKind) !PageId {
            const page_id = try self.pager.allocPage();
            const page_data = try self.pager.getPageForWrite(page_id);
            @memset(page_data, 0);

            const header = @as(*NodeHeader, @ptrCast(@alignCast(page_data.ptr)));
            header.* = NodeHeader{ .kind = kind, .key_count = 0 };

            if (kind == .leaf) {
                @as(*PageId, @ptrCast(@alignCast(&page_data[NEXT_LEAF_OFFSET]))).* = NULL_PAGE;
            }

            return page_id;
        }

        fn insertNonFull(self: *Self, page_id: PageId, key: Key, value: Value, cmp: anytype) !void {
            const page_data = try self.pager.getPageForWrite(page_id);
            const header = @as(*NodeHeader, @ptrCast(@alignCast(&page_data[HEADER_OFFSET])));

            if (header.kind == .leaf) {
                const keys = @as([*]Key, @ptrCast(@alignCast(&page_data[KEYS_OFFSET_LEAF])));
                const values = @as([*]Value, @ptrCast(@alignCast(&page_data[VALUES_OFFSET_LEAF])));
                const key_count = header.key_count;

                const idx = binarySearch(Key, keys[0..key_count], key, cmp);

                if (idx < key_count and cmp(keys[idx], key) == 0) {
                    values[idx] = value;
                    return;
                }

                std.debug.assert(key_count < ORDER_LEAF);

                if (key_count > idx) {
                    std.mem.copyBackwards(Key, keys[idx + 1 .. key_count + 1], keys[idx..key_count]);
                    std.mem.copyBackwards(Value, values[idx + 1 .. key_count + 1], values[idx..key_count]);
                }

                keys[idx] = key;
                values[idx] = value;
                header.key_count = key_count + 1;
            } else {
                const keys = @as([*]Key, @ptrCast(@alignCast(&page_data[KEYS_OFFSET_INTERNAL])));
                const children = @as([*]PageId, @ptrCast(@alignCast(&page_data[CHILDREN_OFFSET_INTERNAL])));
                const key_count = header.key_count;

                const idx = binarySearch(Key, keys[0..key_count], key, cmp);
                var child_page = children[idx];

                const child_data = try self.pager.getPage(child_page);
                const child_header = @as(*const NodeHeader, @ptrCast(@alignCast(&child_data[HEADER_OFFSET]))).*;

                const child_full = switch (child_header.kind) {
                    .leaf => child_header.key_count >= ORDER_LEAF,
                    .internal => child_header.key_count >= ORDER_INTERNAL,
                };

                if (child_full) {
                    try self.splitChild(page_id, idx);

                    const updated_page = try self.pager.getPageForWrite(page_id);
                    const updated_keys = @as([*]const Key, @ptrCast(@alignCast(&updated_page[KEYS_OFFSET_INTERNAL])));
                    const updated_children = @as([*]const PageId, @ptrCast(@alignCast(&updated_page[CHILDREN_OFFSET_INTERNAL])));

                    if (cmp(key, updated_keys[idx]) >= 0) {
                        child_page = updated_children[idx + 1];
                    } else {
                        child_page = updated_children[idx];
                    }
                }

                try self.insertNonFull(child_page, key, value, cmp);
            }
        }

        fn splitRoot(self: *Self) !void {
            const old_root = self.root_page_id;
            const old_data = try self.pager.getPage(old_root);
            const old_header = @as(*const NodeHeader, @ptrCast(@alignCast(&old_data[HEADER_OFFSET]))).*;

            const new_root = try self.createNode(.internal);

            const new_root_data = try self.pager.getPageForWrite(new_root);
            @as([*]PageId, @ptrCast(@alignCast(&new_root_data[CHILDREN_OFFSET_INTERNAL])))[0] = old_root;

            if (old_header.kind == .leaf) {
                const right_sibling = try self.splitLeaf(old_root);

                const root_header = @as(*NodeHeader, @ptrCast(@alignCast(&new_root_data[HEADER_OFFSET])));
                root_header.key_count = 1;

                const root_keys = @as([*]Key, @ptrCast(@alignCast(&new_root_data[KEYS_OFFSET_INTERNAL])));
                const root_children = @as([*]PageId, @ptrCast(@alignCast(&new_root_data[CHILDREN_OFFSET_INTERNAL])));

                const right_data = try self.pager.getPage(right_sibling);
                const right_keys = @as([*]const Key, @ptrCast(@alignCast(&right_data[KEYS_OFFSET_LEAF])));

                root_keys[0] = right_keys[0];
                root_children[1] = right_sibling;
            } else {
                try self.splitChild(new_root, 0);
            }

            self.root_page_id = new_root;
            self.pager.meta.root_page = new_root;
        }

        fn splitLeaf(self: *Self, page_id: PageId) !PageId {
            const right_sibling = try self.createNode(.leaf);

            const left_data = try self.pager.getPageForWrite(page_id);
            const left_header = @as(*NodeHeader, @ptrCast(@alignCast(&left_data[HEADER_OFFSET])));
            const left_keys = @as([*]Key, @ptrCast(@alignCast(&left_data[KEYS_OFFSET_LEAF])));
            const left_values = @as([*]Value, @ptrCast(@alignCast(&left_data[VALUES_OFFSET_LEAF])));

            const key_count = left_header.key_count;
            const split_point = (key_count + 1) / 2;
            const keys_to_move = key_count - split_point;

            const right_data = try self.pager.getPageForWrite(right_sibling);
            const right_header = @as(*NodeHeader, @ptrCast(@alignCast(&right_data[HEADER_OFFSET])));
            const right_keys = @as([*]Key, @ptrCast(@alignCast(&right_data[KEYS_OFFSET_LEAF])));
            const right_values = @as([*]Value, @ptrCast(@alignCast(&right_data[VALUES_OFFSET_LEAF])));

            @memcpy(right_keys[0..keys_to_move], left_keys[split_point..key_count]);
            @memcpy(right_values[0..keys_to_move], left_values[split_point..key_count]);

            right_header.key_count = @intCast(keys_to_move);
            left_header.key_count = @intCast(split_point);

            const old_next = @as(*PageId, @ptrCast(@alignCast(&left_data[NEXT_LEAF_OFFSET]))).*;
            @as(*PageId, @ptrCast(@alignCast(&right_data[NEXT_LEAF_OFFSET]))).* = old_next;
            @as(*PageId, @ptrCast(@alignCast(&left_data[NEXT_LEAF_OFFSET]))).* = right_sibling;

            return right_sibling;
        }

        fn splitChild(self: *Self, parent_id: PageId, child_idx: usize) !void {
            const parent_data = try self.pager.getPageForWrite(parent_id);
            const parent_header = @as(*NodeHeader, @ptrCast(@alignCast(&parent_data[HEADER_OFFSET])));
            const parent_keys = @as([*]Key, @ptrCast(@alignCast(&parent_data[KEYS_OFFSET_INTERNAL])));
            const parent_children = @as([*]PageId, @ptrCast(@alignCast(&parent_data[CHILDREN_OFFSET_INTERNAL])));

            const left_child = parent_children[child_idx];
            const left_data = try self.pager.getPage(left_child);
            const left_header = @as(*const NodeHeader, @ptrCast(@alignCast(&left_data[HEADER_OFFSET]))).*;

            if (left_header.kind == .leaf) {
                const right_sibling = try self.splitLeaf(left_child);

                const right_data = try self.pager.getPage(right_sibling);
                const right_keys = @as([*]const Key, @ptrCast(@alignCast(&right_data[KEYS_OFFSET_LEAF])));
                const separator_key = right_keys[0];

                insertSeparator(parent_header, parent_keys, parent_children, child_idx, separator_key, right_sibling);
            } else {
                const right_sibling = try self.createNode(.internal);

                const left_mut_data = try self.pager.getPageForWrite(left_child);
                const left_hdr = @as(*NodeHeader, @ptrCast(@alignCast(&left_mut_data[HEADER_OFFSET])));
                const left_keys = @as([*]Key, @ptrCast(@alignCast(&left_mut_data[KEYS_OFFSET_INTERNAL])));
                const left_children = @as([*]PageId, @ptrCast(@alignCast(&left_mut_data[CHILDREN_OFFSET_INTERNAL])));

                const key_count = left_hdr.key_count;
                const split_point = key_count / 2;
                const separator_key = left_keys[split_point];
                const keys_to_move = key_count - (split_point + 1);

                const right_data = try self.pager.getPageForWrite(right_sibling);
                const right_header = @as(*NodeHeader, @ptrCast(@alignCast(&right_data[HEADER_OFFSET])));
                const right_keys = @as([*]Key, @ptrCast(@alignCast(&right_data[KEYS_OFFSET_INTERNAL])));
                const right_children = @as([*]PageId, @ptrCast(@alignCast(&right_data[CHILDREN_OFFSET_INTERNAL])));

                if (keys_to_move > 0) {
                    @memcpy(right_keys[0..keys_to_move], left_keys[split_point + 1 .. key_count]);
                    @memcpy(right_children[0 .. keys_to_move + 1], left_children[split_point + 1 .. key_count + 1]);
                } else {
                    right_children[0] = left_children[split_point + 1];
                }

                left_hdr.key_count = @intCast(split_point);
                right_header.key_count = @intCast(keys_to_move);

                insertSeparator(parent_header, parent_keys, parent_children, child_idx, separator_key, right_sibling);
            }
        }

        fn insertSeparator(
            parent_header: *NodeHeader,
            parent_keys: [*]Key,
            parent_children: [*]PageId,
            idx: usize,
            separator: Key,
            right_child: PageId,
        ) void {
            const key_count = parent_header.key_count;
            std.debug.assert(key_count < ORDER_INTERNAL);

            if (key_count > idx) {
                std.mem.copyBackwards(Key, parent_keys[idx + 1 .. key_count + 1], parent_keys[idx..key_count]);
                std.mem.copyBackwards(PageId, parent_children[idx + 2 .. key_count + 2], parent_children[idx + 1 .. key_count + 1]);
            }

            parent_keys[idx] = separator;
            parent_children[idx + 1] = right_child;
            parent_header.key_count = key_count + 1;
        }

        fn binarySearch(comptime T: type, items: []const T, target: T, cmp: anytype) usize {
            if (items.len <= 8) {
                for (items, 0..) |item, i| {
                    if (cmp(item, target) >= 0) return i;
                }
                return items.len;
            }

            var low: usize = 0;
            var high: usize = items.len;
            while (low < high) {
                const mid = low + (high - low) / 2;
                if (cmp(items[mid], target) < 0) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }
    };
}