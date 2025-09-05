const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "zdb",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // Create a module for src files
    const zdb_module = b.addModule("zdb", .{
        .root_source_file = b.path("src/root.zig"),
    });

    // Examples
    const demo_exe = b.addExecutable(.{
        .name = "demo",
        .root_source_file = b.path("examples/demo.zig"),
        .target = target,
        .optimize = optimize,
    });
    demo_exe.root_module.addImport("zdb", zdb_module);
    const demo_run = b.addRunArtifact(demo_exe);
    const demo_step = b.step("demo", "Run demo");
    demo_step.dependOn(&demo_run.step);

    const bench_exe = b.addExecutable(.{
        .name = "benchmark",
        .root_source_file = b.path("examples/benchmark.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_exe.root_module.addImport("zdb", zdb_module);
    const bench_run = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run benchmark");
    bench_step.dependOn(&bench_run.step);

    const test_step = b.step("test", "Run unit tests");
    const tests = b.addTest(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_step.dependOn(&b.addRunArtifact(tests).step);
}